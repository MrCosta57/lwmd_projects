#VARIABLES
inputMain=main.cpp
outputMain=output.out
classes=


#COMPILE
compile:
	g++ -std=c++17 $(inputMain) $(classes) -o $(outputMain) -fopenmp -Wall -Wextra --pedantic -O3 -fsanitize=address -fsanitize=undefined

#CLEAN
clean:
	@rm -f $(outputMain)