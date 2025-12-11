import multiprocessing
import time
import random
import hashlib
import heapq
import queue
import csv
import argparse
import sys
import os
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

# Simulation Constants
MAX_BUFFER_SIZE = 30  # pFabric uses very small buffers (approx 2 BDP)
PACKET_SIZE = 1024
LINK_SPEED_DELAY = 0.0005  # Transmission delay per packet

@dataclass(order=True)
class Packet:
    priority: int
    id: int = field(compare=False)
    src: str = field(compare=False)
    dst: str = field(compare=False)
    flow_id: int = field(compare=False)
    size: int = field(compare=False)
    total_flow_size: int = field(compare=False)
    timestamp: float = field(compare=False)
    type: str = field(default="DATA", compare=False)

# ==========================================
# 2. Traffic Generator
# ==========================================

class TrafficGenerator:
    def __init__(self, mode="MIXED"):
        self.mode = mode

    def get_flow_size(self):
        size = 1
        if self.mode == "SMALL_DOMINATED":
            if random.random() < 0.9: 
                return random.randint(1, 10)
            raw = int(random.paretovariate(alpha=1.5) * 20)
            size = min(raw, 500)
        elif self.mode == "LARGE_DOMINATED":
            if random.random() < 0.2: 
                return random.randint(1, 10)
            raw = int(random.paretovariate(alpha=1.2) * 100)
            size = min(raw, 3000)
        else: # MIXED
            if random.random() < 0.5: 
                return random.randint(1, 20)
            raw = int(random.paretovariate(alpha=1.3) * 50)
            size = min(raw, 2000)
        return max(1, size)

    def get_interarrival_time(self, load=0.5):
        return random.expovariate(lambd=load)

# ==========================================
# 3. Node Classes
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
        self.load_factor = 2.0 

    def run(self):
        time.sleep(0.5)
        
        # Generating 150 flows per host for simulation
        for _ in range(150): 
            if not self.all_destinations: break
            
            dest = random.choice(self.all_destinations)
            if dest == self.name: continue
            
            num_packets = self.traffic_gen.get_flow_size()
            flow_id = random.randint(100000, 999999)
            
            # --- ALGORITHM SPECIFIC FLOW INITIALIZATION ---
            
            path_instruction = None
            assigned_timeslots = []

            if self.algorithm == Algo.FASTPASS:
                # Fastpass: Request timeslots from Arbiter
                # Payload: Source, Destination, Num_Packets, FlowID
                self.controller_queue.put(("REQUEST", self.name, dest, num_packets, flow_id))
                try:
                    # Wait for Arbiter allocation (Path, [Start_Times])
                    msg_type, val = self.reply_queue.get(timeout=5)
                    if msg_type == "GRANT": 
                        path_instruction = val['path']
                        assigned_timeslots = val['timeslots']
                except queue.Empty:
                    # If arbiter doesn't reply, drop flow (or retry logic)
                    continue 

            # Packet Generation Loop
            for i in range(num_packets):
                # pFabric: Priority = Remaining Packets (Shortest Remaining Processing Time)
                # Lower number = Higher priority in heapq
                prio = (num_packets - i) if self.algorithm == Algo.PFABRIC else 0

                pkt = Packet(
                    priority=prio, 
                    id=i, 
                    src=self.name, 
                    dst=dest, 
                    flow_id=flow_id, 
                    size=PACKET_SIZE, 
                    total_flow_size=num_packets, # Pass the flow size here
                    timestamp=time.time()
                )
                
                # --- ALGORITHM SPECIFIC SENDING LOGIC ---

                if self.algorithm == Algo.FASTPASS:
                    if i < len(assigned_timeslots):
                        pkt.type = f"ROUTE:{path_instruction}"
                        target_time = assigned_timeslots[i]
                        now = time.time()
                        wait_time = target_time - now
                        
                        # Enforce the Arbiter's timing (Zero-Queue discipline)
                        if wait_time > 0:
                            time.sleep(wait_time)
                    else:
                        break # Should not happen if protocol holds

                elif self.algorithm == Algo.HEDERA:
                    # Hedera starts as ECMP. Detection happens in switch.
                    pass

                self.link_queue.put(pkt)
                
                # Physical serialization delay
                time.sleep(LINK_SPEED_DELAY) 

            # Inter-flow wait
            time.sleep(self.traffic_gen.get_interarrival_time(self.load_factor))

class Monitor(multiprocessing.Process):
    def __init__(self, queue, filename):
        super().__init__()
        self.queue = queue
        self.filename = filename

    def run(self):
        with open(self.filename, mode='a', newline='') as f:
            writer = csv.writer(f)
            while True:
                try:
                    record = self.queue.get()
                    if record == "STOP": break
                    writer.writerow(record)
                    f.flush()
                except:
                    break

class Switch(multiprocessing.Process):
    def __init__(self, name, input_queue, routes, controller_queue, update_queue, algorithm, monitor_queue):
        super().__init__()
        self.name = name
        self.input_queue = input_queue
        self.routes = routes
        self.controller_queue = controller_queue
        self.update_queue = update_queue
        self.algorithm = algorithm
        self.monitor_queue = monitor_queue
        self.buffer = [] # Priority Queue (min-heap)
        self.hedera_map = {} 
        self.flow_bytes = {}
        # HEDERA specific
        self.hedera_threshold = 20000 # Bytes
        self.packet_count = 0

    def run(self):
        while True:
            # 1. Process Control Plane Updates (Hedera)
            while not self.update_queue.empty():
                cmd, fid, port = self.update_queue.get()
                if cmd == "UPDATE": 
                    self.hedera_map[fid] = port

            # 2. Ingress Processing (Buffer Management)
            fetched_any = False
            # FIX: Fetch batch of packets to allow buffer to fill against the bottleneck
            for _ in range(20):
                try:
                    # Non-blocking check
                    pkt = self.input_queue.get_nowait()
                    fetched_any = True
                except queue.Empty:
                    break

                self.packet_count += 1
                
                # Log Buffer Occupancy periodically (every 20 packets)
                if self.packet_count % 20 == 0:
                    self.monitor_queue.put((time.time(), "BUFFER", self.name, len(self.buffer), self.algorithm.name))

                # --- PFABRIC LOGIC: Priority Dropping ---
                if self.algorithm == Algo.PFABRIC:
                    # If buffer is full
                    if len(self.buffer) >= MAX_BUFFER_SIZE:
                        # Find the "worst" packet in buffer (Largest remaining size = Highest numerical priority)
                        # heapq is min-heap, so we look for max value to drop
                        worst_packet = max(self.buffer, key=lambda p: p.priority)
                        
                        # If incoming packet has smaller remaining size (better priority)
                        if pkt.priority < worst_packet.priority:
                            # Drop worst, accept new
                            self.buffer.remove(worst_packet)
                            self.monitor_queue.put((time.time(), "DROP", self.name, 1, self.algorithm.name))
                            heapq.heapify(self.buffer) # Re-heapify is O(N) but N is small (30)
                            heapq.heappush(self.buffer, pkt)
                        else:
                            # Incoming packet is low priority, drop it (tail drop equivalent)
                            self.monitor_queue.put((time.time(), "DROP", self.name, 1, self.algorithm.name))
                            pass 
                    else:
                        heapq.heappush(self.buffer, pkt)
                
                # --- STANDARD BUFFERING (Tail Drop) ---
                else:
                    if len(self.buffer) < MAX_BUFFER_SIZE:
                        heapq.heappush(self.buffer, pkt)
                    else:
                        # Drop (implicit tail drop)
                        self.monitor_queue.put((time.time(), "DROP", self.name, 1, self.algorithm.name))

            # Sleep briefly if idle to prevent CPU spin
            if not fetched_any and not self.buffer:
                time.sleep(0.0001)

            # 3. Egress Processing (Scheduling)
            if self.buffer:
                # In pFabric, heappop automatically dequeues Smallest Remaining Size (Highest Priority)
                pkt = heapq.heappop(self.buffer)
                
                if pkt.dst not in self.routes: continue

                out_paths = self.routes[pkt.dst]
                out_idx = 0

                # --- ROUTING LOGIC ---

                if self.algorithm == Algo.ECMP:
                    # Static Hash
                    h = int(hashlib.md5(str(pkt.flow_id).encode()).hexdigest(), 16)
                    out_idx = h % len(out_paths)

                elif self.algorithm == Algo.HEDERA:
                    self.flow_bytes[pkt.flow_id] = self.flow_bytes.get(pkt.flow_id, 0) + pkt.size
                    
                    # Check if flow has specific route assigned by Controller
                    if pkt.flow_id in self.hedera_map:
                        out_idx = self.hedera_map[pkt.flow_id]
                    else:
                        # Default to ECMP
                        h = int(hashlib.md5(str(pkt.flow_id).encode()).hexdigest(), 16)
                        out_idx = h % len(out_paths)
                        
                        # Check threshold for "Elephant" detection
                        if self.flow_bytes[pkt.flow_id] > self.hedera_threshold: 
                            # Notify controller to schedule this flow
                            self.controller_queue.put(("HEDERA_DETECT", pkt.flow_id, pkt.src, pkt.dst))
                            # Prevent spamming controller for same flow
                            self.flow_bytes[pkt.flow_id] = -999999999 

                elif self.algorithm == Algo.FASTPASS:
                    # Arbiter assigned path is in packet header
                    if "ROUTE" in pkt.type:
                        _, val = pkt.type.split(":")
                        out_idx = int(val) % len(out_paths)
                
                # pFabric uses standard shortest path (or ECMP) for routing, 
                # but relies on the Priority Queueing (implemented above in Egress pop)
                elif self.algorithm == Algo.PFABRIC:
                    h = int(hashlib.md5(str(pkt.flow_id).encode()).hexdigest(), 16)
                    out_idx = h % len(out_paths)

                # Send
                try:
                    # FIX: Simulate Transmission Delay (Bottleneck)
                    time.sleep(LINK_SPEED_DELAY)
                    out_paths[out_idx].put(pkt)
                except IndexError: pass

class GlobalController(multiprocessing.Process):
    def __init__(self, req_queue, switch_queues, host_queues, algorithm):
        super().__init__()
        self.req_queue = req_queue
        self.switch_queues = switch_queues
        self.host_queues = host_queues 
        self.algorithm = algorithm
        
        # State for Hedera (Load estimation)
        self.path_load = {0: 0, 1: 0} # Simplified tracking of 2 paths
        
        # State for Fastpass (Timeslot allocation)
        # map: link_id -> next_free_timestamp
        self.link_schedule = {} 

    def run(self):
        while True:
            try:
                msg = self.req_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            # --- HEDERA: Global First Fit / Simulated Annealing ---
            if self.algorithm == Algo.HEDERA and msg[0] == "HEDERA_DETECT":
                _, flow_id, src, dst = msg
                
                # Simple Global First Fit Heuristic:
                # Assign flow to the path with currently lower estimated load.
                # In a real implementation, we would query switches for counters.
                # Here we approximate by tracking assignments.
                
                selected_path = 0
                if self.path_load[1] < self.path_load[0]:
                    selected_path = 1
                
                self.path_load[selected_path] += 1
                
                # Install rule in switches
                for sw_q in self.switch_queues.values():
                    sw_q.put(("UPDATE", flow_id, selected_path))

            # --- FASTPASS: Centralized Arbitration ---
            elif self.algorithm == Algo.FASTPASS and msg[0] == "REQUEST":
                _, src, dst, num_packets, flow_id = msg
                
                # 1. Path Selection (Load Balancing)
                # Fastpass selects path to minimize latency. Simple implementation: Round Robin or Random
                path = random.choice([0, 1])
                
                # 2. Timeslot Allocation
                # Calculate when the packets can actually traverse the link without queuing
                timeslots = []
                current_time = time.time()
                
                # We track the "next free time" for the shared bottleneck link (Receiver)
                link_id = "UPLINK_TO_RECV" 
                if link_id not in self.link_schedule:
                    self.link_schedule[link_id] = current_time
                
                start_time = max(current_time, self.link_schedule[link_id])
                
                for i in range(num_packets):
                    # Schedule packet
                    slot = start_time + (i * LINK_SPEED_DELAY)
                    timeslots.append(slot)
                
                # Update link schedule
                self.link_schedule[link_id] = timeslots[-1] + LINK_SPEED_DELAY
                
                # Send Grant to Host
                if src in self.host_queues:
                    response = {
                        'path': path,
                        'timeslots': timeslots
                    }
                    self.host_queues[src].put(("GRANT", response))

class Receiver(multiprocessing.Process):
    def __init__(self, input_queue, filename, algo_name, workload_name, monitor_queue):
        super().__init__()
        self.input_queue = input_queue
        self.filename = filename
        self.algo_name = algo_name
        self.workload_name = workload_name
        self.monitor_queue = monitor_queue
        self.total_bytes = 0
        self.start_time = time.time()

    def run(self):
        with open(self.filename, mode='a', newline='') as f:
            writer = csv.writer(f)
            while True:
                try:
                    pkt = self.input_queue.get(timeout=5)
                    self.total_bytes += pkt.size
                    # FCT calculation
                    fct = time.time() - pkt.timestamp
                    writer.writerow([self.algo_name, self.workload_name, pkt.flow_id, pkt.total_flow_size, fct])
                    f.flush()
                except queue.Empty:
                    # Timeout suggests simulation end
                    duration = time.time() - self.start_time
                    if duration > 0:
                        throughput_mbps = (self.total_bytes * 8) / (duration * 1_000_000)
                        self.monitor_queue.put((time.time(), "THROUGHPUT", "Receiver", throughput_mbps, self.algo_name))
                    break

# ==========================================
# 4. Topology & Helpers
# ==========================================

def merger(q_in1, q_in2, q_out):
    """
    Simulates a physical link merger or switch fabric aggregation.
    """
    while True:
        try: 
            pkt = q_in1.get_nowait(); q_out.put(pkt)
        except queue.Empty: pass
        try: 
            pkt = q_in2.get_nowait(); q_out.put(pkt)
        except queue.Empty: pass
        time.sleep(0.0001)

def build_topology(algorithm, workload_type, output_file, stats_file):
    # Link Queues
    h0_sw1 = multiprocessing.Queue()
    h1_sw1 = multiprocessing.Queue()
    sw1_sw2_path1 = multiprocessing.Queue()
    sw1_sw2_path2 = multiprocessing.Queue()
    sw2_recv = multiprocessing.Queue()
    
    # Control Plane Queues
    ctrl_req = multiprocessing.Queue()
    sw1_update = multiprocessing.Queue()
    
    # Host Reply Queues (For Fastpass Grants)
    h0_reply_q = multiprocessing.Queue()
    h1_reply_q = multiprocessing.Queue()

    # Monitor Queue
    monitor_q = multiprocessing.Queue()
    monitor = Monitor(monitor_q, stats_file)

    # Routing Tables
    # Receiver is reachable via two paths from Switch 1
    sw1_routes = { "Receiver": [sw1_sw2_path1, sw1_sw2_path2] }
    sw2_routes = { "Receiver": [sw2_recv] }
    
    # Controller Setup
    host_queues_map = {"Host_A": h0_reply_q, "Host_B": h1_reply_q}
    ctl = GlobalController(ctrl_req, {'Switch1': sw1_update}, host_queues_map, algorithm)
    
    # Switch 1 (Ingress Switch)
    # Merges inputs from Host A and B
    shared_uplink = multiprocessing.Queue()
    
    # We use a merger helper to simulate simultaneous arrival at switch port buffers
    sw1_input_merger = multiprocessing.Process(target=merger, args=(h0_sw1, h1_sw1, shared_uplink))
    
    sw1 = Switch("Switch1", shared_uplink, sw1_routes, ctrl_req, sw1_update, algorithm, monitor_q)
    
    # Switch 2 (Core/Egress Switch)
    # Merges the two paths back into one uplink to receiver
    sw1_sw2_merged = multiprocessing.Queue()
    sw2_input_merger = multiprocessing.Process(target=merger, args=(sw1_sw2_path1, sw1_sw2_path2, sw1_sw2_merged))
    sw2 = Switch("Switch2", sw1_sw2_merged, sw2_routes, ctrl_req, multiprocessing.Queue(), algorithm, monitor_q)
    
    recv = Receiver(sw2_recv, output_file, algorithm.value, workload_type, monitor_q)

    # Hosts
    destinations = ["Receiver"]
    h0 = Host("Host_A", destinations, h0_sw1, ctrl_req, h0_reply_q, algorithm, workload_type)
    h1 = Host("Host_B", destinations, h1_sw1, ctrl_req, h1_reply_q, algorithm, workload_type)

    return [monitor, ctl, sw1_input_merger, sw1, sw2_input_merger, sw2, recv, h0, h1]

# ==========================================
# 5. Main Entry Point
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--algo", type=str, required=True, help="ECMP, HEDERA, FASTPASS, PFABRIC")
    parser.add_argument("--workload", type=str, required=True, help="MIXED, SMALL_DOMINATED, LARGE_DOMINATED")
    parser.add_argument("--out", type=str, required=True, help="CSV output filename")
    parser.add_argument("--stats", type=str, required=False, default="stats.csv", help="Stats output filename")
    parser.add_argument("--seed", type=int, required=False, default=None, help="Random seed")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    try:
        algo_enum = Algo[args.algo.upper()]
    except KeyError:
        print(f"Error: Algorithm {args.algo} not found.")
        sys.exit(1)
    
    try:
        with open(args.out, 'a', newline='') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["Algorithm", "Workload", "FlowID", "Size", "FCT"])
    except Exception: pass

    # Initialize stats file
    if not os.path.exists(args.stats):
        with open(args.stats, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Metric", "Entity", "Value", "Algorithm"])

    processes = build_topology(algo_enum, args.workload, args.out, args.stats)
    
    for p in processes: p.start()

    # Simulation Runtime
    time.sleep(60) 
    
    for p in processes: 
        p.terminate()
        p.join()
    
    sys.exit(0)