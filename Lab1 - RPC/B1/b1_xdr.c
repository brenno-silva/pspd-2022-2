/*
 * Please do not edit this file.
 * It was generated using rpcgen.
 */

#include "b1.h"

bool_t
xdr_palavras (XDR *xdrs, palavras *objp)
{
	register int32_t *buf;

	 if (!xdr_pointer (xdrs, (char **)&objp->palavra, sizeof (char), (xdrproc_t) xdr_char))
		 return FALSE;
	 if (!xdr_int (xdrs, &objp->qtd))
		 return FALSE;
	return TRUE;
}