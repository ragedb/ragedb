# Relationship and Property definitions for pyragedb-semantics
import re

def parse_reading(reading_str):
    # Find placeholders like {Concept:Person} or {Primitive:String:name}
    pattern = r"\{([^}]+)\}"
    placeholders = re.findall(pattern, reading_str)

    if len(placeholders) < 1:
        raise ValueError("A reading must contain at least a subject placeholder.")

    if len(placeholders) == 1:
        subject_parts = placeholders[0].split(":")
        subject_name = subject_parts[1] if len(subject_parts) > 1 else subject_parts[0]
        first_placeholder_full = "{" + placeholders[0] + "}"
        verb_text = reading_str.replace(first_placeholder_full, "").strip()
        inferred_name = re.sub(r'[^a-zA-Z0-9_]', '_', verb_text).lower()
        inferred_name = re.sub(r'_+', '_', inferred_name).strip('_')
        return {
            "subject_name": subject_name,
            "object_type": "Primitive",
            "object_name": "Boolean",
            "attr_name": inferred_name,
            "verb_text": verb_text
        }

    subject_parts = placeholders[0].split(":")
    object_parts = placeholders[-1].split(":")

    subject_type = subject_parts[0]  # "Concept" or "Primitive"
    subject_name = subject_parts[1]

    object_type = object_parts[0]
    object_name = object_parts[1]

    role = None
    if len(object_parts) > 2:
        role = object_parts[2]

    # Extract literal text between first and last placeholders
    first_placeholder_full = "{" + placeholders[0] + "}"
    last_placeholder_full = "{" + placeholders[-1] + "}"

    start_idx = reading_str.find(first_placeholder_full) + len(first_placeholder_full)
    end_idx = reading_str.rfind(last_placeholder_full)

    verb_text = reading_str[start_idx:end_idx].strip()

    # Clean verb text for inferred name
    inferred_name = re.sub(r'[^a-zA-Z0-9_]', '_', verb_text).lower()
    inferred_name = re.sub(r'_+', '_', inferred_name).strip('_')

    attr_name = role if role else inferred_name

    return {
        "subject_name": subject_name,
        "object_type": object_type,
        "object_name": object_name,
        "attr_name": attr_name,
        "verb_text": verb_text
    }


class Property:
    def __init__(self, reading):
        self.reading = reading
        self.parsed = parse_reading(reading)
        self.is_relationship = False
        self.subject = self.parsed["subject_name"]
        self.target = self.parsed["object_name"]
        self.name = self.parsed["attr_name"]

    def __call__(self, other=None):
        from .concept import AttributeRef
        from .model import _active_model
        # Look up the subject concept in the model's concept catalog index
        subject_concept = _active_model.concept_index.get(self.subject)
        ref = AttributeRef(subject_concept, self.name, is_relationship=False, target=self.target)
        if other is not None:
            return ref == other
        return ref

    def ref(self, name=None):
        return self().ref(name)



class Relationship:
    def __init__(self, reading):
        self.reading = reading
        self.parsed = parse_reading(reading)
        self.is_relationship = True
        self.subject = self.parsed["subject_name"]
        self.target = self.parsed["object_name"]
        self.name = self.parsed["attr_name"]

    def __call__(self, other=None):
        from .concept import Concept, AttributeRef
        from .model import _active_model
        # Look up both subject and target concepts in the model's concept catalog index
        subject_concept = _active_model.concept_index.get(self.subject)
        target_concept = _active_model.concept_index.get(self.target)
        ref = AttributeRef(subject_concept, self.name, is_relationship=True, target=target_concept)
        if other is not None:
            if isinstance(other, (Concept, AttributeRef)):
                return ref
            return ref == other
        return ref

    def ref(self, name=None):
        return self().ref(name)

