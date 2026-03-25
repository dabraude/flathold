"""Per-tag display and grouping (stored in ``db/tag_definitions``)."""

from dataclasses import dataclass

from flathold.tag_group import TagGroup


@dataclass(frozen=True, slots=True)
class TagRuleMetadata:
    """Metadata for a kebab-case transaction tag."""

    counter_party: bool
    calculated: bool
    groups: tuple[TagGroup, ...]
