def idempotency_gate(source_n: int, dest_n: int, batch_key_src, batch_key_dest) -> dict:
    ok = source_n == dest_n and batch_key_src == batch_key_dest
    return {
        "source_count": source_n,
        "dest_count": dest_n,
        "batch_key_match": batch_key_src == batch_key_dest,
        "gate_passed": ok,
    }
