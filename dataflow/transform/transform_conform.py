import logging
import apache_beam as beam
from transform import transform_functions as transform_rules_module


def handle_transform_rules(message, transformation_rules, entity_name):
    config = transformation_rules[entity_name]
    element = None
    transforms = config.get('transforms')
    if message is not None:
        if transforms is not None:
            for transform in transforms:
                try:
                    argument_list = transform.get('arguments')
                    if argument_list is None:
                        argument_list = {}
                    for arguments in argument_list:
                        message = getattr(transform_rules_module, transform['function'])(message, **arguments)
                    element = message
                except (ValueError, TypeError, AttributeError) as err:
                    logging.debug(f'Transformation exception:{err}')
                    pass
        else:
            element = message
            logging.debug(f'Transformation element:{str(element)}')
        return element
    else:
        return None

class ApplyTransformConformRules(beam.PTransform):
    def __init__(self, transformation_rules, entity_name):
        self.transformation_rules = transformation_rules
        self.entity_name = entity_name

    def expand(self, input_or_inputs):
        return (
            input_or_inputs | 'Apply transform rules' >> beam.Map(handle_transform_rules, self.transformation_rules, self.entity_name)
        )