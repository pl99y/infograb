"""Disk maintenance shared by both export profiles."""


def compact_database_if_needed(con):
    """Reuse free pages until reclaiming them would materially shrink the file."""
    page_size = con.execute("PRAGMA page_size").fetchone()[0]
    page_count = con.execute("PRAGMA page_count").fetchone()[0]
    free_pages = con.execute("PRAGMA freelist_count").fetchone()[0]
    if free_pages * page_size >= 4 * 1024 * 1024 and free_pages * 4 >= page_count:
        con.execute("VACUUM")
        print("[ok] vacuumed database (reclaimed reusable pages)")
