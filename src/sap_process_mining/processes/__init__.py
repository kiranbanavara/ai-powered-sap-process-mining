from .o2c import O2C_ACTIVITIES, O2C_DIMENSIONS, O2C_HAPPY_PATH, ProcessDefinition, o2c_process
from .p2p import P2P_ACTIVITIES, P2P_DIMENSIONS, P2P_HAPPY_PATH, p2p_process


# Single place the CLI / UI / analytics look up processes by slug.
PROCESSES: dict[str, ProcessDefinition] = {
    o2c_process.slug: o2c_process,
    p2p_process.slug: p2p_process,
}


def get_process(slug: str) -> ProcessDefinition:
    try:
        return PROCESSES[slug]
    except KeyError:
        raise ValueError(
            f"Unknown process '{slug}'. Available: {sorted(PROCESSES)}"
        ) from None


__all__ = [
    "ProcessDefinition",
    "PROCESSES",
    "get_process",
    "o2c_process",
    "O2C_ACTIVITIES",
    "O2C_HAPPY_PATH",
    "O2C_DIMENSIONS",
    "p2p_process",
    "P2P_ACTIVITIES",
    "P2P_HAPPY_PATH",
    "P2P_DIMENSIONS",
]
