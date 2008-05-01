#if !defined(_dummy_assoc_h_)
#define _dummy_assoc_h_

#include "items.h"

extern item* assoc_find(const char* key, const size_t nkey);
int assoc_insert(item *item);
item* assoc_update(item *it);
void assoc_delete(const char *key, const size_t nkey);

#endif /* #if !defined(_dummy_assoc_h_) */
