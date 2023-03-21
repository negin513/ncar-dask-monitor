import os

if os.geteuid() == 0:
    print("You have sudo access")
else:
    print("You don't have sudo access")

