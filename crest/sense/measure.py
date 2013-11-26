__author__ = 'chriswhsu'


class Measure:
    name = None
    description = None
    uom = None
    datatype = None

    def __init__(self, name,
                 description,
                 uom,
                 datatype):

        """ initialize new measure object
        """

        self.name = name
        self.description = description
        self.uom = uom
        self.datatype = datatype

    def __str__(self):
        return 'measure name: %s'%self.name