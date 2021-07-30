from operator import attrgetter
import time
from typing import Dict, List, Tuple

# Custom types
EndpointPair = Tuple[int, int]
SwitchPortPair = Tuple[int, int]
Path = List[Tuple[int, int, int]]


def update_link_used_bw(tx_bytes: int, last_tx_bytes: int, clock: float) -> float:
    return (tx_bytes - last_tx_bytes) * 8.0 / (time.time() - clock) / 1000


def update_link_available_bw(bw: float, used_bw: float) -> float:
    return int(bw) * 1024.0 - used_bw


def update_bw(msg, switches: List[int], adjacency: Dict[SwitchPortPair, int], bw: Dict[EndpointPair, float],
              available_bw: Dict[EndpointPair, float], used_bw: Dict[EndpointPair, float],
              tx_bytes: Dict[EndpointPair, int], clock: Dict[EndpointPair, float]) -> (
        Dict[EndpointPair, float], Dict[EndpointPair, float], Dict[EndpointPair, int], Dict[EndpointPair, float]):
    body = msg.body
    dpid = msg.datapath.id
    for stat in sorted(body, key=attrgetter('port_no')):
        for switch in switches:
            if adjacency[dpid, switch] == stat.port_no:
                if tx_bytes[dpid, switch] > 0:
                    used_bw[dpid, switch] = update_link_used_bw(stat.tx_bytes, tx_bytes[dpid, switch],
                                                                clock[dpid, switch])
                    available_bw[dpid, switch] = update_link_available_bw(bw[dpid, switch], used_bw[dpid, switch])
                tx_bytes[dpid, switch] = stat.tx_bytes
                clock[dpid, switch] = time.time()
    return used_bw, available_bw, tx_bytes, clock
