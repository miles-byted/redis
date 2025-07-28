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

#include <string.h>
#include "trie.h"
#include "zmalloc.h"
#include "adlist.h"

/* ========================== Internal helper functions ========================== */

/* Create a new Trie node */
static trie_node *trie_node_new(void) {
    /* Using zcalloc is safer as it initializes memory to zero */
    trie_node *node = (trie_node *)zcalloc(sizeof(trie_node));
    /* zcalloc has already set is_end_of_word, data, and children to 0/NULL */
    return node;
}

/* Check if a node has no children */
static int is_empty(trie_node *node) {
    if (!node) return 1;
    for (int i = 0; i < TRIE_CHAR_SET_SIZE; i++) {
        if (node->children[i]) return 0;
    }
    return 1;
}

/* Recursively free Trie node helper function */
static void trie_node_free(trie_node *node, trie_data_free_func free_func) {
    if (!node) return;
    for (int i = 0; i < TRIE_CHAR_SET_SIZE; i++) {
        trie_node_free(node->children[i], free_func);
    }
    /* If the user provided a data free function and the current node has associated data, call it */
    if (free_func && node->data) {
        free_func(node->data);
    }
    zfree(node);
}

/* Recursively delete key helper function
 * Returns: 1 if the current node curr should be deleted by its parent, 0 otherwise */
static int trie_delete_recursive(trie_node *curr, sds key, size_t key_len, size_t depth, void **deleted_data) {
    if (!curr) return 0; /* Path does not exist */

    /* Reached the end of the key */
    if (depth == key_len) {
        if (curr->is_end_of_word) {
            curr->is_end_of_word = 0;
            *deleted_data = curr->data;
            curr->data = NULL;
            /* If this node has no children, it can be safely deleted */
            return is_empty(curr);
        }
        return 0; /* Not the end of a word, do nothing */
    }

    /* Recursively go to the next node */
    int index = (unsigned char)key[depth];
    if (trie_delete_recursive(curr->children[index], key, key_len, depth + 1, deleted_data)) {
        /* Child node was deleted */
        zfree(curr->children[index]);
        curr->children[index] = NULL;

        /* If the current node is not the end of a word and has no other children, it should also be deleted */
        return !curr->is_end_of_word && is_empty(curr);
    }

    return 0; /* Child node was not deleted, so the current node cannot be deleted either */
}

/* ========================== Public API implementation ========================== */

/*
 * Create a new Trie
 */
trie *trie_new(void) {
    trie *t = (trie *)zmalloc(sizeof(trie));
    if (!t) return NULL;
    
    t->root = trie_node_new();
    if (!t->root) {
        zfree(t);
        return NULL;
    }
    t->size = 0;
    return t;
}

/*
 * Free a Trie
 */
void trie_free(trie *t, trie_data_free_func free_func) {
    if (!t) return;
    trie_node_free(t->root, free_func);
    zfree(t);
}

/*
 * Insert a key into the Trie
 */
int trie_insert(trie *t, sds key, void *data) {
    if (!t || !t->root || !key) return 0;
    
    trie_node *curr = t->root;
    size_t key_len = sdslen(key);
    for (size_t i = 0; i < key_len; i++) {
        int index = (unsigned char)key[i];
        if (!curr->children[index]) {
            curr->children[index] = trie_node_new();
            if (!curr->children[index]) return 0; /* Memory allocation failed */
        }
        curr = curr->children[index];
    }
    
    if (!curr->is_end_of_word) {
        t->size++;
    }
    curr->is_end_of_word = 1;
    curr->data = data;
    return 1;
}

/*
 * Lookup a key in the Trie
 */
void* trie_lookup(trie *t, sds key) {
    if (!t || !t->root || !key) return NULL;
    
    trie_node *curr = t->root;
    size_t key_len = sdslen(key);
    for (size_t i = 0; i < key_len; i++) {
        int index = (unsigned char)key[i];
        curr = curr->children[index];
        if (!curr) return NULL; /* Not found */
    }
    
    return curr->is_end_of_word ? curr->data : NULL;
}

/*
 * Delete a key from the Trie
 */
void* trie_delete(trie *t, sds key) {
    if (!t || !t->root || !key) return NULL;
    void *deleted_data = NULL;
    size_t key_len = sdslen(key);

    if (key_len == 0) {
        if (t->root->is_end_of_word) {
            deleted_data = t->root->data;
            t->root->is_end_of_word = 0;
            t->root->data = NULL;
            t->size--;
        }
        return deleted_data;
    }

    int index = (unsigned char)key[0];
    if (t->root->children[index] != NULL) {
        if (trie_delete_recursive(t->root->children[index], key, key_len, 1, &deleted_data)) {
            zfree(t->root->children[index]);
            t->root->children[index] = NULL;
        }
    }
    if (deleted_data) t->size--;
    return deleted_data;
}

/*
 * Count the number of nodes in the Trie
 */
size_t trie_size(trie *t) {
    if (!t) return 0;
    return t->size;
}

/*
 * Create a new Trie iterator
 */
trie_iterator *trie_iterator_new(trie *t, sds text) {
    if (!t || !text) return NULL;
    
    trie_iterator *it = (trie_iterator *)zmalloc(sizeof(trie_iterator));
    if (!it) return NULL;
    
    it->root = t->root;
    it->text = text;
    it->text_len = sdslen(text);
    it->text_pos = 0;
    it->ends_with_star = 0;
    
    /* Initialize internal state */
    it->current_match_node = t->root;
    it->current_match_offset = 0;
    
    return it;
}

/*
 * Free a Trie iterator
 */
void trie_iterator_free(trie_iterator *it) {
    zfree(it);
}

void trie_iterator_set_ends_with_star(trie_iterator *it, int enabled) {
    it->ends_with_star = enabled;
}

/*
 * Get the next match from the Trie iterator.
 * This iterator finds all prefixes of the given text that are present in the trie.
 * For example, if the trie contains "a", "ap", "apple" and the text is "applepie",
 * the iterator will return "a", "ap", "apple" in successive calls.
 * It performs a prefix search, not a substring search.
 */
void* trie_iterator_next(trie_iterator *it, sds *key) {
    if (!it || it->text_pos >= it->text_len) {
        return NULL;
    }

    // Special handling:
    // If currently at the beginning of the text and the root node marks the end of a word,
    // return a match for an empty string.
    if (it->current_match_offset == 0 && it->root->is_end_of_word) {
        if (it->ends_with_star) {
            *key = sdsnewlen("*", 1);
        } else {
            *key = sdsempty();
        }
        it->current_match_offset++; // Prevent returning this match again next time
        return it->root->data;
    }

    size_t scan_pos = it->text_pos + it->current_match_offset;

    while (scan_pos < it->text_len) {
        char c = it->text[scan_pos];
        int index = (unsigned char)c;

        /* If there is no child for the current character, it means no more prefixes can be found. */
        if (!it->current_match_node || !it->current_match_node->children[index]) {
            it->text_pos = it->text_len; /* Mark iterator as finished. */
            return NULL;
        }

        it->current_match_node = it->current_match_node->children[index];
        it->current_match_offset++;
        scan_pos++;

        /* If the new node marks the end of a word, we have found a match. */
        if (it->current_match_node->is_end_of_word) {
            size_t keylen = it->current_match_offset;
            if (it->ends_with_star) {
                *key = sdsnewlen(SDS_NOINIT, keylen + 1);
                if (!*key) return NULL; /* OOM */
                memcpy(*key, it->text + it->text_pos, keylen);
                (*key)[keylen] = '*';
            } else {
                *key = sdsnewlen(it->text + it->text_pos, keylen);
            }
            return it->current_match_node->data;
        }
    }

    /* Reached the end of the text. */
    it->text_pos = it->text_len; /* Mark iterator as finished. */
    return NULL;
}
