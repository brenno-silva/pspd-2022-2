/*
 * This is sample code generated by rpcgen.
 * These are only templates and you can use them
 * as a guideline for developing your own functions.
 */

#include "b1.h"
#include <stdio.h>
#include <string.h>

char *passaPalavra(char **textFile, CLIENT *clnt)
{
	char **palavrasContadas;

	palavrasContadas = contapalavra_1(textFile, clnt);

	if (palavrasContadas == NULL)
	{
		fprintf(stderr, "Problema na chamada RPC\n");
		exit(0);
	}

	return *palavrasContadas;
}

int main(int argc, char *argv[])
{
	CLIENT *clnt;
	FILE *file = fopen("entrada.txt", "r");
	char *textFile = 0;
	long fileLength;

	if (file == NULL)
	{
		fprintf(stderr, "Erro ao abrir arquivo\n");
		return 0;
	}

	fseek(file, 0, SEEK_END);
	fileLength = ftell(file);
	fseek(file, 0, SEEK_SET);
	textFile = malloc(fileLength);

	if (textFile)
		fread(textFile, 1, fileLength, file);

	fclose(file);

	if (argc != 2)
	{
		fprintf(stderr, "Uso: %s hostname\n", argv[0]);
		exit(0);
	}

	clnt = clnt_create(argv[1], PROG, VERSAO, "tcp");

	if (clnt == (CLIENT *)NULL)
	{
		clnt_pcreateerror(argv[1]);
		exit(1);
	}

	textFile = passaPalavra(&textFile, clnt);

	printf("%s\n", textFile);

	free(textFile);

	return (0);
}
