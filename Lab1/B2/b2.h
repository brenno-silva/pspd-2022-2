/*
 * Please do not edit this file.
 * It was generated using rpcgen.
 */

#ifndef _B2_H_RPCGEN
#define _B2_H_RPCGEN

#include <rpc/rpc.h>


#ifdef __cplusplus
extern "C" {
#endif


struct palavras {
	char *palavra;
	int qtd;
};
typedef struct palavras palavras;

#define PROG 1
#define VERSAO 1

#if defined(__STDC__) || defined(__cplusplus)
#define contaPalavra 1
extern  char ** contapalavra_1(char **, CLIENT *);
extern  char ** contapalavra_1_svc(char **, struct svc_req *);
extern int prog_1_freeresult (SVCXPRT *, xdrproc_t, caddr_t);

#else /* K&R C */
#define contaPalavra 1
extern  char ** contapalavra_1();
extern  char ** contapalavra_1_svc();
extern int prog_1_freeresult ();
#endif /* K&R C */

/* the xdr functions */

#if defined(__STDC__) || defined(__cplusplus)
extern  bool_t xdr_palavras (XDR *, palavras*);

#else /* K&R C */
extern bool_t xdr_palavras ();

#endif /* K&R C */

#ifdef __cplusplus
}
#endif

#endif /* !_B2_H_RPCGEN */
