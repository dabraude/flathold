"""Concrete `TAG_RULES` list."""

from flathold.tag_rules.core import TagRule, validate_kebab_tag
from flathold.tag_rules.rules_counter_party import TAG_RULES_COUNTER_PARTY
from flathold.tag_rules.rules_entertainment import TAG_RULES_ENTERTAINMENT
from flathold.tag_rules.rules_food_groceries import TAG_RULES_FOOD_GROCERIES
from flathold.tag_rules.rules_home_finance import TAG_RULES_HOME_FINANCE
from flathold.tag_rules.rules_personal import TAG_RULES_PERSONAL

TAG_RULES: tuple[TagRule, ...] = (
    *TAG_RULES_PERSONAL,
    *TAG_RULES_HOME_FINANCE,
    *TAG_RULES_ENTERTAINMENT,
    *TAG_RULES_FOOD_GROCERIES,
    *TAG_RULES_COUNTER_PARTY,
)

_rule_tags = [r.tag for r in TAG_RULES]
if len(_rule_tags) != len(set(_rule_tags)):
    dup = [t for t in set(_rule_tags) if _rule_tags.count(t) > 1]
    msg = f"TAG_RULES must use unique tag names; duplicates: {sorted(dup)!r}"
    raise ValueError(msg)
for r in TAG_RULES:
    validate_kebab_tag(r.tag)
