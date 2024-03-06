import random

# initialising an array
array = []

# iterating from 1 to 1000 using for loop
for i in range(1000):
    # creating a random number at each iteration
    randomNum = random.randint(0, 9)
    # appending it in the array
    array.append(randomNum)

# creating variable for each number, this will store the number count in each given variable
num0 = 0
num1 = 0
num2 = 0
num3 = 0
num4 = 0
num5 = 0
num6 = 0
num7 = 0
num8 = 0
num9 = 0

# using if and elif to add to each number count
for num in array:
    if num == 0:
        num0 += 1
    elif num == 1:
        num1 += 1
    elif num == 2:
        num2 += 1
    elif num == 3:
        num3 += 1
    elif num == 4:
        num4 += 1
    elif num == 5:
        num5 += 1
    elif num == 6:
        num6 += 1
    elif num == 7:
        num7 += 1
    elif num == 8:
        num8 += 1
    elif num == 9:
        num9 += 1

# using dictionary to store number of counts of each number and using string interpolation.
numCountArray = {"0": f"{num0} times",
                 "1": f"{num1} times",
                 "2": f"{num2} times",
                 "3": f"{num3} times",
                 "4": f"{num4} times",
                 "5": f"{num5} times",
                 "6": f"{num6} times",
                 "7": f"{num7} times",
                 "8": f"{num8} times",
                 "9": f"{num9} times"}

# printing array and array number count
print("This is the array of numbers", array)
print()
# You can either display it using this in different lines
print("This is the count of each number in the array")
for key in numCountArray.items():
    print(key)
print()
# or this on to print on the same line
print("This is the count of each number in the array\n", numCountArray)