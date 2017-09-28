"""scratch
__author__ = davidwrench

Created = 9/26/17

Description: # TODO: Description...


Usage: # TODO: Usage...


Example: # TODO: Example...
"""


def get_evens():
    evens = []
    for _ in range(20):
        if _ % 2 == 0:
            evens.append(_)
    return evens


def print_evens():
    print(get_evens())

print_evens()
