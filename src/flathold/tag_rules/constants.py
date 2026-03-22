"""Shared constants for tag rule predicates."""

# AVIVA row at this |Debit + Credit| amount also gets tag `david-life-insurance`.
DAVID_LIFE_INSURANCE_AVIVA_AMOUNT = 20.72

# Tesco: TESCO then space, hyphen, or underscore, then STORES or GROCERY; optional -digits suffix.
TESCO_STORES_OR_GROCERY = r"(?i)TESCO[-_\s]+(STORES|GROCERY)(-\d+)?"
