MPICC ?= mpicc

all: fsfrag-write

fsfrag-write:
	$(MPICC) -g -std=c99 -Wall fsfrag-write.c -o fsfrag-write


clean:
	@rm -f fsfrag-write

