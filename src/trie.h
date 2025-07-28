/*
 * Copyright (c) 2023, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __TRIE_H
#define __TRIE_H

#include <stddef.h> /* For size_t */
#include "sds.h"
#include "adlist.h"

/* Define character set size, usually 256 for ASCII */
#define TRIE_CHAR_SET_SIZE 256

/* Trie node structure */
typedef struct trie_node {
    int is_end_of_word;             /* Mark if it's the end of a word */
    void *data;                     /* Store data associated with the word */
    struct trie_node *children[TRIE_CHAR_SET_SIZE]; /* Array of child node pointers */
} trie_node;

/* Trie tree structure */
typedef struct trie {
    trie_node *root; /* Root node */
    size_t size;     /* Number of words in the trie */
} trie;

/* Trie iterator structure */
typedef struct trie_iterator {
    trie_node *root;                /* Pointer to the root of the trie */
    sds text;                       /* Text to search in */
    size_t text_len;                /* Length of the text */
    size_t text_pos;                /* Current search start position in the text */
    int ends_with_star;             /* Add '*' to the end of the returned key */

    /* Internal state to support next calls */
    trie_node *current_match_node;  /* Currently matched node */
    size_t current_match_offset;    /* Current match offset */
} trie_iterator;

/* Callback function pointer type for freeing user data */
typedef void (*trie_data_free_func)(void *data);


/* ========================== Function declarations ========================== */

/* Trie tree core functions */
trie *trie_new(void);
void trie_free(trie *t, trie_data_free_func free_func);
int trie_insert(trie *t, sds key, void *data);
void* trie_lookup(trie *t, sds key);
void* trie_delete(trie *t, sds key);
size_t trie_size(trie *t);

/* Trie iterator functions */
trie_iterator *trie_iterator_new(trie *t, sds text);
void trie_iterator_set_ends_with_star(trie_iterator *it, int enabled);
void trie_iterator_free(trie_iterator *it);
void* trie_iterator_next(trie_iterator *it, sds *key);

#endif /* __TRIE_H */
