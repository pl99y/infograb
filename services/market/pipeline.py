from __future__ import annotations

from collectors.common import utc_now_iso
from services.market.queries import get_market_snapshots_with_meta
from storage import get_connection, insert_market_snapshot, record_fetch_run


MARKET_FETCH_RUN_NAME = "Market/Snapshots"


def fetch_market_snapshots_once(db_path: str = "app.db", timeout: int = 15) -> dict:
    """
    Fetch live market snapshots, store successful items, and record one fetch run.

    Storage strategy:
    - Each successful index snapshot is inserted as a new row.
    - Latest API queries read the newest successful row per key.
    - Partial failures keep older successful rows for failed keys.
    """
    started_at = utc_now_iso()
    result = get_market_snapshots_with_meta(timeout=timeout)
    items = result.get("items", []) or []
    errors = result.get("errors", []) or []

    con = get_connection(db_path)
    try:
        for item in items:
            insert_market_snapshot(
                con,
                snapshot_key=item["key"],
                name=item["name"],
                symbol=item["symbol"],
                price=item.get("price"),
                change_percent=item.get("change_percent"),
                source_name=item.get("source") or "unknown",
                fetched_at=started_at,
            )

        if items and not errors:
            status = "success"
        elif items and errors:
            status = "partial"
        else:
            status = "failed"

        error_message = "; ".join(
            f"{err.get('key')}: {err.get('error')}" for err in errors if isinstance(err, dict)
        )

        record_fetch_run(
            con=con,
            source_id=None,
            source_name=MARKET_FETCH_RUN_NAME,
            started_at=started_at,
            finished_at=started_at,
            status=status,
            items_found=len(items) + len(errors),
            new_items=len(items),
            error_message=error_message,
        )
        con.commit()
    finally:
        con.close()

    return {
        "fetched_at": started_at,
        "status": status,
        "items_saved": len(items),
        "errors": errors,
    }
