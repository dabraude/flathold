"""Seed rows for ``db/tag_definitions`` (default when the table is created)."""

# (tag, counter_party, groups as tuple of TagGroup value strings).
TAG_DEFINITIONS_SEED_ROWS: tuple[tuple[str, bool, tuple[str, ...]], ...] = (
    ("dog-food", False, ()),
    ("us", False, ()),
    ("pets", False, ()),
    ("groomer", False, ()),
    ("insurance", False, ()),
    ("david-life-insurance", False, ()),
    ("health-insurance", False, ()),
    ("vitality", False, ("counter-party",)),
    ("house-insurance", False, ()),
    ("printer", False, ()),
    ("gas-and-electricity", False, ()),
    ("hyperoptic", False, ("counter-party",)),
    ("utilities", False, ("sector-codes",)),
    ("ring", False, ("counter-party",)),
    ("edinburgh-council", False, ("counter-party",)),
    ("council-tax", False, ()),
    ("tax", False, ()),
    ("factors", False, ()),
    ("mortgage", False, ()),
    ("loans", False, ("sector-codes",)),
    ("car", False, ()),
    ("cleaning", False, ("sector-codes",)),
    ("fresh-car", False, ("counter-party",)),
    ("kitchen", False, ()),
    ("disney", False, ("counter-party",)),
    ("netflix", False, ("counter-party",)),
    ("octopus", False, ("counter-party",)),
    ("youtube-premium", False, ()),
    ("entertainment", False, ("sector-codes",)),
    ("bbc", False, ("counter-party",)),
    ("tv-license", False, ()),
    ("eating-out", False, ()),
    ("williams-and-johnson", False, ("counter-party",)),
    ("dish-washer-tablets", False, ()),
    ("groceries", False, ("sector-codes",)),
    ("farmers-market", False, ("counter-party",)),
    ("abbis-pantry", False, ("counter-party",)),
    ("agata", False, ("counter-party",)),
    ("waitrose", False, ("counter-party",)),
    ("great-grog", False, ("counter-party",)),
    ("google", False, ("counter-party",)),
    ("field-doctor", False, ("counter-party",)),
    ("tescos", False, ("counter-party",)),
    ("dave", False, ("counter-party",)),
    ("claire", False, ("counter-party",)),
    ("chocolaterium", False, ("counter-party",)),
    ("aviva", False, ("counter-party",)),
    ("amazon", False, ("counter-party",)),
    ("amazon-spend", False, ()),
    ("borlands", False, ("counter-party",)),
    ("pet-plan", False, ("counter-party",)),
    ("hp", False, ("counter-party",)),
    ("ross-and-liddell", False, ("counter-party",)),
    ("smol", False, ("counter-party",)),
    ("natwest", False, ("counter-party",)),
    ("bos", False, ("counter-party",)),
    ("toyota-finance", False, ("counter-party",)),
    ("wren", False, ("counter-party",)),
    ("years", False, ("counter-party",)),
    ("cash", False, ("counter-party",)),
    ("cash-spend", False, ("cash-transactions",)),
    ("cash-withdrawal", False, ()),
    ("unknown-cash", False, ("cash-transactions",)),
    ("untagged-spend", False, ()),
    ("uncategorised-sector", False, ("sector-codes",)),
)

CALCULATED_TAG_NAMES: frozenset[str] = frozenset(
    {"unknown-cash", "untagged-spend", "uncategorised-sector"}
)


def seed_groups_for_tag(tag: str) -> tuple[str, ...]:
    """Groups tuple for ``tag`` from the seed table (empty if unknown)."""
    for t, _cp, groups in TAG_DEFINITIONS_SEED_ROWS:
        if t == tag:
            return groups
    return ()
