struct palavras {
	char* palavra;
    int qtd;
};


program PROG {
	version VERSAO {
		string contaPalavra(string) = 1;
	} = 1;
} = 1;
