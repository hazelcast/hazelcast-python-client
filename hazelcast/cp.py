class RaftGroupId(object):
    __slots__ = ("name", "seed", "id")

    def __init__(self, name, seed, group_id):
        self.name = name
        self.seed = seed
        self.id = group_id

    def __eq__(self, other):
        return isinstance(other, RaftGroupId) \
               and self.name == other.name \
               and self.seed == other.seed \
               and self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.name, self.seed, self.id))
