import logging
import torch

from lib.utils import torch_equals_ignore_index

logger = logging.getLogger(__name__)


def get_accuracy(targets, outputs, ignore_index=None, print_=False):
    """
    Args:
      targets (list of tensors)
      outputs (list of tensors)
    """
    n_correct = 0
    for target, output in zip(targets, outputs):
        target = target.squeeze(dim=0)
        output = output.squeeze(dim=0)

        prediction = output.max(output.dim() - 1)[1].view(-1)
        
        if torch_equals_ignore_index(target, prediction, ignore_index=ignore_index):
            n_correct += 1
    accuracy = float(n_correct) / len(targets)
    if print_:
        logger.info('Accuracy: %s [%d of %d]', accuracy, n_correct, len(targets))
    return accuracy, n_correct, len(targets)
