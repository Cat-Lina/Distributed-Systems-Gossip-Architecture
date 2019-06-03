# Client.py
import Pyro4
FrontEnd = Pyro4.Proxy("PYRONAME:FrontEnd")    # use name server object lookup uri shortcut      # get a Pyro proxy to the Hello object
name = input("What is your name? ").strip()
print ("Response: ", FrontEnd.sayHello(name))   # call remote method
choice=""
while True and choice!=5:
    print("Menu: ")
    print("1. Submit movie rating")
    print("2. Request movie rating")
    print("5. enter 5 to quit")

    valid=False
    while (valid==False):
        try:
            choice = int(input("Please enter a number: "))
            if ((choice ==1) or (choice==2) or (choice==5)):
                valid=True
        except ValueError:
            print("Oops!  That was no valid number.  Try again...")
    valid=False
    while(valid==False):
        if (choice==1):
            try:
                userID = int(input("Please enter your user id: "))
                movieID = int(input("Please enter a movie id: "))
                rating= float(input("Please enter a movie rating between 0-5: "))
                if (rating>5.0 or rating<0.0):
                    print("Enter rating between 0 and 5!!")
                    continue
                valid=True
            except ValueError:
                print("Enter integer for id and int/float for rating")
                continue
            
            submit=FrontEnd.submitRating(userID,movieID,rating)
            print(submit)
        elif (choice==2):
            try:
                movieID = int(input("Please enter a movie id: "))
                valid=True
            except ValueError:
                print("Enter integer value please!")
                continue
            request=FrontEnd.requestRating(movieID)
            if request!="undefined":
                print ("The average rating given to movieID ", movieID ," is " , request)   # call remote method
            else:
                print ("No rating submitted by user to movie ", movieID)