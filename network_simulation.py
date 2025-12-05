import multiprocessing
import time
import random
import hashlib
import heapq
import queue
import csv
import argparse
import sys
from dataclasses import dataclass, field
from enum import Enum

# ==========================================
# 1. Configuration & Data Structures
# ==========================================

class Algo(Enum):
    ECMP = "ECMP"
    HEDERA = "HEDERA"
    FASTPASS = "FASTPASS"
    PFABRIC = "PFABRIC"

@dataclass(order=True)
class Packet:
    priority: int
    id: int = field(compare=False)
    src: str = field(compare=False)
    dst: str = field(compare=False)
    flow_id: int = field(compare=False)
    size: int = field(compare=False)
    type: str = field(compare=False, default="DATA")
    timestamp: float = field(compare=False, default=0.0)

# ==========================================
# 2. Traffic Generator (STABILITY UPDATE)
# ==========================================

class TrafficGenerator:
    def __init__(self, mode="MIXED"):
        self.mode = mode

    def get_flow_size(self):
        """
        Returns flow size with a hard CAP to prevent simulation freeze.
        """
        size = 1
        
        if self.mode == "SMALL_DOMINATED":
            # Mostly mice
            if random.random() < 0.9: 
                return random.randint(1, 10)
            # Elephants (capped at 500 pkts)
            raw = int(random.paretovariate(alpha=1.5) * 20)
            size = min(raw, 500)
            
        elif self.mode == "LARGE_DOMINATED":
            # Mostly elephants
            if random.random() < 0.2: 
                return random.randint(1, 10)
            # Elephants (capped at 3000 pkts to prevent infinite loops)
            raw = int(random.paretovariate(alpha=1.2) * 100)
            size = min(raw, 3000)
            
        else: # MIXED
            if random.random() < 0.5: 
                return random.randint(1, 20)
            # Elephants (capped at 2000 pkts)
            raw = int(random.paretovariate(alpha=1.3) * 50)
            size = min(raw, 2000)
        
        return max(1, size)

    def get_interarrival_time(self, load=0.5):
        return random.expovariate(lambd=load)

# ==========================================
# 3. Node Classes (STABILITY UPDATE)
# ==========================================

class Host(multiprocessing.Process):
    def __init__(self, name, all_destinations, link_queue, controller_queue, reply_queue, algorithm, workload_type):
        super().__init__()
        self.name = name
        self.all_destinations = all_destinations
        self.link_queue = link_queue
        self.controller_queue = controller_queue
        self.reply_queue = reply_queue 
        self.algorithm = algorithm
        self.traffic_gen = TrafficGenerator(workload_type)
        # Load factor 2.0 = ~0.5s average gap between flows
        self.load_factor = 2.0 

    def run(self):
        time.sleep(0.5)
        
        # --- UPDATE: Increased from 15 to 150 flows ---
        # This provides a large enough sample size to smooth out variances
        for _ in range(150): 
            if not self.all_destinations: break
            
            dest = random.choice(self.all_destinations)
            if dest == self.name: continue
            
            num_packets = self.traffic_gen.get_flow_size()
            flow_id = random.randint(100000, 999999)
            
            path_instruction = None
            if self.algorithm == Algo.FASTPASS:
                # Send only name and ID (Registry Pattern)
                self.controller_queue.put((self.name, flow_id))
                try:
                    msg_type, val = self.reply_queue.get(timeout=2)
                    if msg_type == "GRANT": path_instruction = val
                except queue.Empty:
                    continue 

            for i in range(num_packets):
                # pFabric: Priority = Remaining Packets
                prio = (num_packets - i) if self.algorithm == Algo.PFABRIC else 0
                
                pkt = Packet(
                    priority=prio, id=i, src=self.name, dst=dest, 
                    flow_id=flow_id, size=1024, timestamp=time.time()
                )
                
                if path_instruction is not None:
                    pkt.type = f"ROUTE:{path_instruction}"

                self.link_queue.put(pkt)
                # 0.5ms serialization delay (simulating 20Gbps link roughly)
                time.sleep(0.0005) 

            # Wait for Poisson arrival
            time.sleep(self.traffic_gen.get_interarrival_time(self.load_factor))

class Switch(multiprocessing.Process):
    def __init__(self, name, input_queue, routes, controller_queue, update_queue, algorithm):
        super().__init__()
        self.name = name
        self.input_queue = input_queue
        self.routes = routes
        self.controller_queue = controller_queue
        self.update_queue = update_queue
        self.algorithm = algorithm
        self.buffer = [] 
        self.hedera_map = {} 
        self.flow_bytes = {}

    def run(self):
        while True:
            # Hedera Updates
            while not self.update_queue.empty():
                cmd, fid, port = self.update_queue.get()
                if cmd == "UPDATE": self.hedera_map[fid] = port

            # Ingress
            try:
                if not self.buffer:
                    pkt = self.input_queue.get(timeout=2)
                    heapq.heappush(self.buffer, pkt)
                while not self.input_queue.empty():
                    pkt = self.input_queue.get_nowait()
                    heapq.heappush(self.buffer, pkt)
            except queue.Empty:
                continue

            # Egress
            if self.buffer:
                pkt = heapq.heappop(self.buffer)
                if pkt.dst not in self.routes: continue

                out_paths = self.routes[pkt.dst]
                out_idx = 0

                if self.algorithm == Algo.ECMP:
                    h = int(hashlib.md5(str(pkt.flow_id).encode()).hexdigest(), 16)
                    out_idx = h % len(out_paths)

                elif self.algorithm == Algo.HEDERA:
                    self.flow_bytes[pkt.flow_id] = self.flow_bytes.get(pkt.flow_id, 0) + pkt.size
                    if pkt.flow_id in self.hedera_map:
                        out_idx = self.hedera_map[pkt.flow_id]
                    else:
                        h = int(hashlib.md5(str(pkt.flow_id).encode()).hexdigest(), 16)
                        out_idx = h % len(out_paths)
                        if self.flow_bytes[pkt.flow_id] > 20000: 
                            self.controller_queue.put(("HEDERA_DETECT", pkt.flow_id))

                elif self.algorithm == Algo.FASTPASS:
                    if "ROUTE" in pkt.type:
                        _, val = pkt.type.split(":")
                        out_idx = int(val) % len(out_paths)

                try:
                    out_paths[out_idx].put(pkt)
                except IndexError: pass

class GlobalController(multiprocessing.Process):
    def __init__(self, req_queue, switch_queues, host_queues, algorithm):
        super().__init__()
        self.req_queue = req_queue
        self.switch_queues = switch_queues
        self.host_queues = host_queues 
        self.algorithm = algorithm

    def run(self):
        while True:
            try:
                msg = self.req_queue.get(timeout=3)
            except queue.Empty:
                continue

            if self.algorithm == Algo.HEDERA and msg[0] == "HEDERA_DETECT":
                _, flow_id = msg
                # Hedera logic: assign to path 1
                for sw_q in self.switch_queues.values():
                    sw_q.put(("UPDATE", flow_id, 1))

            elif self.algorithm == Algo.FASTPASS:
                src, fid = msg
                # Fastpass logic: round robin path assignment
                path = random.choice([0, 1])
                if src in self.host_queues:
                    self.host_queues[src].put(("GRANT", path))

class Receiver(multiprocessing.Process):
    def __init__(self, input_queue, filename, algo_name, workload_name):
        super().__init__()
        self.input_queue = input_queue
        self.filename = filename
        self.algo_name = algo_name
        self.workload_name = workload_name

    def run(self):
        with open(self.filename, mode='a', newline='') as f:
            writer = csv.writer(f)
            while True:
                try:
                    pkt = self.input_queue.get(timeout=5)
                    fct = time.time() - pkt.timestamp
                    writer.writerow([self.algo_name, self.workload_name, pkt.flow_id, pkt.size, fct])
                    f.flush()
                except queue.Empty:
                    break

# ==========================================
# 4. Topology & Helpers
# ==========================================

def merger(q_in1, q_in2, q_out):
    while True:
        try: 
            pkt = q_in1.get_nowait(); q_out.put(pkt)
        except queue.Empty: pass
        try: 
            pkt = q_in2.get_nowait(); q_out.put(pkt)
        except queue.Empty: pass
        time.sleep(0.001)

def build_topology(algorithm, workload_type, output_file):
    # Links
    h0_sw1 = multiprocessing.Queue()
    h1_sw1 = multiprocessing.Queue()
    sw1_sw2_path1 = multiprocessing.Queue()
    sw1_sw2_path2 = multiprocessing.Queue()
    sw2_recv = multiprocessing.Queue()
    
    ctrl_req = multiprocessing.Queue()
    sw1_update = multiprocessing.Queue()
    
    # Host Reply Queues (Created here to solve Pickling error)
    h0_reply_q = multiprocessing.Queue()
    h1_reply_q = multiprocessing.Queue()

    # Routes
    sw1_routes = { "Receiver": [sw1_sw2_path1, sw1_sw2_path2] }
    sw2_routes = { "Receiver": [sw2_recv] }
    
    # Processes
    host_queues_map = {"Host_A": h0_reply_q, "Host_B": h1_reply_q}
    ctl = GlobalController(ctrl_req, {'Switch1': sw1_update}, host_queues_map, algorithm)
    
    shared_uplink = multiprocessing.Queue()
    sw1 = Switch("Switch1", shared_uplink, sw1_routes, ctrl_req, sw1_update, algorithm)
    
    sw1_sw2_merged = multiprocessing.Queue()
    merger_proc = multiprocessing.Process(target=merger, args=(sw1_sw2_path1, sw1_sw2_path2, sw1_sw2_merged))
    sw2 = Switch("Switch2", sw1_sw2_merged, sw2_routes, ctrl_req, multiprocessing.Queue(), algorithm)
    
    recv = Receiver(sw2_recv, output_file, algorithm.value, workload_type)

    # Hosts
    destinations = ["Receiver"]
    h0 = Host("Host_A", destinations, shared_uplink, ctrl_req, h0_reply_q, algorithm, workload_type)
    h1 = Host("Host_B", destinations, shared_uplink, ctrl_req, h1_reply_q, algorithm, workload_type)

    return [ctl, sw1, merger_proc, sw2, recv, h0, h1]

# ==========================================
# 5. Main Entry Point (STABILITY UPDATE)
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--algo", type=str, required=True, help="ECMP, HEDERA, FASTPASS, PFABRIC")
    parser.add_argument("--workload", type=str, required=True, help="MIXED, SMALL_DOMINATED, LARGE_DOMINATED")
    parser.add_argument("--out", type=str, required=True, help="CSV output filename")
    args = parser.parse_args()

    algo_enum = Algo[args.algo]
    
    try:
        with open(args.out, 'x', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Algorithm", "Workload", "FlowID", "Size", "FCT"])
    except FileExistsError:
        pass

    processes = build_topology(algo_enum, args.workload, args.out)
    
    for p in processes: p.start()

    # --- UPDATE: Increased timeout to 100s ---
    # This allows the 150 flows per host to finish processing.
    time.sleep(100) 
    
    for p in processes: 
        p.terminate()
        p.join()
    
    sys.exit(0)