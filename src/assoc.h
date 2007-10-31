/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#if !defined(_assoc_h_)
#define _assoc_h_
/* associative array */
void assoc_init(void);
item *assoc_find(const char *key, const size_t nkey);
int assoc_insert(item *item);
void assoc_delete(const char *key, const size_t nkey);
void do_assoc_move_next_bucket(void);
uint32_t hash( const void *key, size_t length, const uint32_t initval);
int do_assoc_expire_regex(char *pattern);
#endif /* #if !defined(_assoc_h_) */
