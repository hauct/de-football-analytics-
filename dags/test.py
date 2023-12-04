import os
import sys

print(os.path.abspath(__file__))
print(os.path.dirname(os.path.abspath(__file__)))
print(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))