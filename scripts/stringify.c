#include <stdio.h>

int main(int argc, char **argv)
{
	int c, count = 0;
	if (argc != 2)
		return 1;

	printf("const char %s[] = { ", argv[1]);
	while ((c = fgetc(stdin)) > 0) {
		count++;
		printf("0x%02x, ", c);
	}
	printf("0 };\n");

	return 0;
}
