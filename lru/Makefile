##############################################################
#               CMake Project Wrapper Makefile               #
############################################################## 

#RHEL_VER := $(shell uname -r | grep -o -E '(el5|el6)')
#ifeq ($(RHEL_VER), el5)
#  PATH     := /s/gcc-4.6.1/bin:$(PATH)
#endif
#ifeq ($(RHEL_VER), el6)
#  PATH     := /s/gcc-4.6.2/bin:$(PATH)
#endif
#export PATH

all:
	cd src;\
	g++ -std=c++0x *.cpp exceptions/*.cpp -I. -Wall -o dbms_main -g

clean:
	cd src;\
	rm -f dbms_main test.?

#doc:
#	doxygen Doxyfile
