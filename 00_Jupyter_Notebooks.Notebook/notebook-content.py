# Synapse Analytics notebook source


# MARKDOWN ********************

# # Jupyter notebooks + quick Python refresher

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### Why use Jupyter Notebooks? They make it very handy to:
# - Try out code and experiment with it
# - Combine code with plots of your data
# - Annotate your code and findings with markdown, just like this text.
# - You don't need to rerun your script every time, but can choose parts of it: variables are kept in memory

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### You can run code in Jupyter Notebook by using the shortcut <kbd>Shift</kbd> + <kbd>Enter</kbd> inside the code cell.<br>Let's write a welcome text assign it to a variable and use `print()` to get the output

# CELL ********************

welcome_text = 'Welcome to this introductory course on python for data analysis'
print(welcome_text)

# MARKDOWN ********************

# ### But Jupyter notebook doesn't need the `print()` statement to print a variable. It will print the string representation of your variable when you just type it and then execute it with `Shift + Enter`. See what happens when you just run the code below:

# CELL ********************

welcome_text

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### What is also very handy, is the <kbd>Ctrl</kbd> + <kbd>Space</kbd> inside functions: it shows you all the arguments of that function. See what happens for yourself if you use <kbd>Ctrl</kbd> + <kbd>Space</kbd> inside the `print()` function below:

# CELL ********************

print()

# MARKDOWN ********************

# ### You can also get information about the documentation of the function, by putting a question mark `?` in front of it and running the cell. Please try below:

# CELL ********************

?print

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### Good to know is that you also run simple command line statements and other with so-called **magic commands**. They have a `%` in front of the statement. Below is an example with `ls`. This is not a python statement but it will run nevertheless.

# CELL ********************

%ls -al

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### Converting your cell from code to markdown can be done by pressing <kbd>Ctrl</kbd> + <kbd>m</kbd> followed by <kbd>m</kbd> (you first press <kbd>Ctrl</kbd> + <kbd>m</kbd>, release, then press <kbd>m</kbd>):

# MARKDOWN ********************

# #### Just writing some markdown
# - bullet 1
# - bullet 2

# MARKDOWN ********************

# ### Let's try writing some markdown:

# CELL ********************


# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### You can create new code cells by pressing <kbd>Ctrl</kbd> + <kbd>m</kbd> followed by <kbd>a</kbd> (a stands for above). This is very much like an emacs code editor. Please try to add some new code cells.

# MARKDOWN ********************

# ### The same goes for adding a new code cell below. This can be done by <kbd>Ctrl</kbd> + <kbd>m</kbd> followed by <kbd>b</kbd> (b stands for below)

# MARKDOWN ********************

# ### Deleting cells can be done with <kbd>Ctrl</kbd> + <kbd>m</kbd> followed by <kbd>d</kbd>. Please delete the cells you just added above or below:

# CELL ********************


# CELL ********************


# CELL ********************


# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### Python has list over which you can easily iterate. Notice the 4 space indentation (or 1 tab).<br>Let's write a fruit list `['apples', 'banana', 2, 'oranges']` and loop over it.

# CELL ********************

my_fruits_list = ['apples', 'banana', 2, 'oranges']

for fruit in my_fruits_list:
    print(fruit)

# MARKDOWN ********************

# ### This is very different from other programming languages where you might write something like this. Here you can also see python's focus on ease and readability:

# CELL ********************

for i in range(0, len(my_fruits_list)):
    print(my_fruits_list[i])

# MARKDOWN ********************

# ### Or even something like this:

# CELL ********************

i = 0

while i < len(my_fruits_list):
    print(my_fruits_list[i])
    i += 1 

# MARKDOWN ********************

# ### Slicing lists using brackets. Slicing starts at 0

# CELL ********************

my_fruits_list[0:2]

# MARKDOWN ********************

# ### Getting the last element of a list

# CELL ********************

my_fruits_list[-1]

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### Python dictionary: key value pairs, unordered.

# CELL ********************

my_fruits_dict = {
    'first': ['apples', 'oranges'],
    'second': 'banana',
    'third': 3
}

my_fruits_dict['second']

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ### Defining a function with `def`:

# CELL ********************

### Defining a function with def

def add_two_numbers(a, b):
    """Returns the sum of 2 numbers"""
    return a + b

number1 = 2
number2 = 3

add_two_numbers(2, 3)
