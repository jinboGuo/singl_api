# 该方法用来封装断言，目前包括等于，大于等于，小于等于，不为空，包含
import unittest


class CheckPoints(unittest.TestCase):
    def __init__(self):
        self.flag = 0

    def equal(self, a, b, msg):
        try:
            self.assertEqual(a, b, msg)
        except:
            self.flag += 1

    def greater(self, a, b, msg):
        try:
            self.assertGreater(a, b, msg )
        except:
            self.flag += 1

    def greater_equal(self, a, b, msg):
        try:
            self.assertGreaterEqual(a, b, msg)
        except:
            self.flag += 1

    def less(self, a, b, msg):
        try:
            self.assertLess(a, b, msg)
        except:
            self.flag += 1

    def less_equal(self,a, b, msg):
        try:
            self.assertLessEqual(a, b, msg)
        except:
            self.flag += 1

    def notNone(self, a, msg):
        try:
            self.assertIsNotNone(a, msg)
        except:
            self.flag += 1

    def include(self,a, b, msg):
        try:
            self.assertIn(a, b, msg)
        except:
            self.flag += 1

    def result(self, message):
        if self.flag > 0:
            self.assertTrue(False, message)


class Tas(unittest.TestCase):
    def test_01(self):
        a = 3
        b = 4
        CheckPoints().greater(b, a, '错误')
        self.assertEqual(a, b, '88')
    def test_02(self):
        a = 33
        b = 4
        CheckPoints().equal(b, a, '错误')

if __name__ == '__main__':
    unittest.main()
