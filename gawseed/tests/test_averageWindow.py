import unittest

class averageWindowTests(unittest.TestCase):
    def test_loading(self):
        from averageWindow import AverageWindow
        self.assertTrue(True, "loaded ok")

        aw = AverageWindow()
        self.assertTrue(aw is not None, "created ok")

    def test_w1m0(self):
        from averageWindow import AverageWindow
        aw = AverageWindow(window_size=1, middle_size=0)

        aw.add_element(0)
        aw.add_element(10)

        self.assertTrue(aw.get_lhs() == 0, "LHS is 0")
        self.assertTrue(aw.get_rhs() == 10, "RHS is 10")
        self.assertTrue(aw.get_delta() == 10, "delta is 10")
 
        aw.add_element(5)
        self.assertTrue(aw.get_lhs() == 10, "LHS is 10")
        self.assertTrue(aw.get_rhs() == 5,  "RHS is 5")
        self.assertTrue(aw.get_delta() == -5, "delta is -5")
        
    def test_w1m1(self):
        from averageWindow import AverageWindow
        aw = AverageWindow(window_size=1, middle_size=1)

        aw.add_element(0)
        aw.add_element(5)
        aw.add_element(10)

        self.assertEqual(aw.get_lhs(), 0, "LHS is 0")
        self.assertEqual(aw.get_rhs(), 10, "RHS is 10")
        self.assertEqual(aw.get_delta(), 10, "delta is 10")
 
        aw.add_element(25)
        self.assertEqual(aw.get_lhs(), 5, "LHS is 5")
        self.assertEqual(aw.get_rhs(), 25,  "RHS is 25")
        self.assertEqual(aw.get_delta(), 20, "delta is 20")
        
    def test_w2m1(self):
        from averageWindow import AverageWindow
        aw = AverageWindow(window_size=2, middle_size=1)

        aw.add_element(-1)
        aw.add_element(1)
        aw.add_element(5)
        aw.add_element(0)
        aw.add_element(20)


        self.assertEqual(aw.get_lhs(), 0, "LHS is 0")
        self.assertEqual(aw.get_rhs(), 20, "RHS is 10")
        self.assertEqual(aw.get_delta(), 20, "delta is 10")
 
        aw.add_element(40)
        self.assertEqual(aw.get_lhs(), 1+5, "LHS is 1+5")
        self.assertEqual(aw.get_rhs(), 20+40,  "RHS is 20+40")
        self.assertEqual(aw.get_delta(), 20+40 - (1+5),
                         "delta is 20+40 - (1+5)")
        
