#!/usr/bin/python

"""contains the AverageWindow class implement time-series window averaging

Classes:
    - AverageWindow

"""

class AverageWindow(object):
    """Handles the creation of a sliding window with the ability 
    to quickly average each half as elements are added.

    As the data window slides along a signal with an edge, 
    the left and right hand side averages shift accordingly.

         lhs  |  rhs
       1 1 1 1 9 9 9 9


       lhs average = 4 * 1 / 4 = 1, rhs = 9 * 4 / 4 = 9

    To simplify the math, we assume if we're looking for a step 
    up of M in size, rather than doing the above division we instead 
    do the math without the division and scale M instead:

       lhs average = 4 * 1 = 4, rhs = 9 * 4 = 36

       and then scale M by 4 to become M*4 for searching

    As we add a signal to the right of the time series, 
    data moves from the right window into the left:

         lhs  |  rhs
       1 1 1 9 9 9 9 5

    Thus:

      lhs_total = previous_ltotal - previous_lleft + previous_rleft
      rhs_total = previous_rtotal - previous_rleft + new_val

    The middle_size parameter can be used to add a window inbetween
    the two averages that is ignored to help detect ramping better:
    
         lhs  |  middle |rhs
       1 1 1 1 2 3 5 6 7 9 9 9 9

    Usage:

      aw = AverageWindow(window_size = 4)
      for n in range(1,4):
        aw.add_element(1)
      for n in range(1,9):
        aw.add_element(9)

      lhs = aw.lhs()
      rhs = aw.rhs()
      delta = aw.delta() # == rhs-lhs

    Note: data is initially seeded with 0s and likely unusable
    averages will result until window_size * 2 + middle_size 
    elements have been added.

"""

    def __init__(self, window_size = 5, middle_size = 0):
        self._window_size = window_size
        self._middle_size = middle_size
        self.lhs_data = []
        self.rhs_data = []
        self.middle_data = []
        self.lhs = 0
        self.rhs = 0
        self.middle = 0
        self.index = 0

        # we store data in cyclic arrays for speed

        for num in range(0, window_size):
            self.lhs_data.append(0)
            self.rhs_data.append(0)
        for num in range(0, middle_size):
            self.middle_data.append(0)


    @property
    def window_size(self):
        return self._window_size

    @property
    def middle_size(self):
        return self._middle_size


    def add_element(self, new_element):
        """Add an element to the current slot, stored in a rotating buffer"""

        # update the total counts
        if self._middle_size > 0:
            self.lhs = self.lhs - self.lhs_data[self.index] + self.middle_data[self.middle]
            self.lhs_data[self.index] = self.middle_data[self.middle]
            self.middle_data[self.middle] = self.rhs_data[self.index]

            # update the middle index
            self.middle = self.middle + 1
            if self.middle >= self._middle_size:
                self.middle = 0

        else:
            self.lhs = self.lhs - self.lhs_data[self.index] + self.rhs_data[self.index]
            self.lhs_data[self.index] = self.rhs_data[self.index]

        # rhs side updating is always the same regardless of middle
        self.rhs = self.rhs - self.rhs_data[self.index] + new_element

        # update the array data
        self.rhs_data[self.index] = new_element

        # handle lhs and rhs index cycling
        self.index = self.index + 1
        if self.index >= self._window_size:
            self.index = 0

    def get_lhs(self):
        "Get the left hand averaged window value."
        return self.lhs

    def get_rhs(self):
        "Get the right hand averaged window value."
        return self.rhs

    def get_delta(self):
        "Get the delta between the windows: right minus the left."
        return self.rhs - self.lhs

    def get_data(self):
        "Get an array containing all the stored data."
        return self.lhs_data + self.middle_data + self.rhs_data

    def dump(self):
        "Debugging routine to print the contents of the current data"
        print("----")
        print("size: " + str(self._window_size))
        print("indx: " + str(self.index))
        print("left: " + str(self.lhs_data))
        print("rght: " + str(self.rhs_data))

        print("msiz: " + str(self._middle_size))
        print("mid:  " + str(self.middle_data))

        print("lhs:  " + str(self.get_lhs()))
        print("rhs:  " + str(self.get_rhs()))
        print("dlta: " + str(self.get_delta()))

if __name__ == "__main__":
    pass
    
