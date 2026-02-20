#include <stdalign.h>
#include <stdint.h>
#include <stdio.h>

#include "packed_array.h"
#include "string_op.h"
#include "test.h"

typedef struct {
  int id;
} Item;

typedef struct {
  int count;
  int sum;
} CleanupCtx;

static void cleanup_item(void *obj, void *ctx) {
  Item *it = (Item *)obj;
  CleanupCtx *c = (CleanupCtx *)ctx;
  c->count++;
  c->sum += it->id;
}

static void test_basic_emplace_and_at(void) {
  PackedArray *a = parr_create(sizeof(Item));
  ASSERT_TRUE(a != NULL);
  ASSERT_TRUE(parr_len(a) == 0);

  Item *p0 = NULL;
  Item *p1 = NULL;
  uint32_t i0 = parr_emplace(a, (void **)&p0);
  uint32_t i1 = parr_emplace(a, (void **)&p1);
  ASSERT_TRUE(i0 == 0);
  ASSERT_TRUE(i1 == 1);
  ASSERT_TRUE(parr_len(a) == 2);

  p0->id = 10;
  p1->id = 20;

  Item *r0 = (Item *)parr_at(a, 0);
  const Item *r1 = (const Item *)parr_cat(a, 1);
  ASSERT_TRUE(r0->id == 10);
  ASSERT_TRUE(r1->id == 20);

  parr_destroy(a);
}

static void test_drop_swap(void) {
  PackedArray *a = parr_create(sizeof(Item));
  Item *p = NULL;
  parr_emplace(a, (void **)&p);
  p->id = 1;
  parr_emplace(a, (void **)&p);
  p->id = 2;
  parr_emplace(a, (void **)&p);
  p->id = 3;
  ASSERT_TRUE(parr_len(a) == 3);

  parr_drop_swap(a, 1);
  ASSERT_TRUE(parr_len(a) == 2);

  Item *r0 = (Item *)parr_at(a, 0);
  Item *r1 = (Item *)parr_at(a, 1);
  ASSERT_TRUE(r0->id == 1);
  ASSERT_TRUE(r1->id == 3);

  parr_destroy(a);
}

static void test_cleanup_drop_and_destroy(void) {
  PackedArray *a = parr_create(sizeof(Item));
  CleanupCtx ctx = {0};
  parr_set_cleanup(a, cleanup_item, &ctx);

  Item *p = NULL;
  parr_emplace(a, (void **)&p);
  p->id = 4;
  parr_emplace(a, (void **)&p);
  p->id = 5;
  parr_emplace(a, (void **)&p);
  p->id = 6;

  parr_drop_swap(a, 0); // cleanup should see id=4
  ASSERT_TRUE(ctx.count == 1);
  ASSERT_TRUE(ctx.sum == 4);

  parr_destroy(a); // cleanup remaining 2 items
  ASSERT_TRUE(ctx.count == 3);
  ASSERT_TRUE(ctx.sum == 4 + 5 + 6);
}

static void test_alignment(void) {
  PackedArray *a = parr_create(sizeof(max_align_t));
  ASSERT_TRUE(a != NULL);

  void *p = NULL;
  parr_emplace(a, &p);
  ASSERT_TRUE(p != NULL);
  ASSERT_TRUE(((uintptr_t)p % alignof(max_align_t)) == 0);

  parr_destroy(a);
}

static void test_invalid_inputs(void) {
  PackedArray *a = parr_create(0);
  ASSERT_TRUE(a == NULL);

  a = parr_create(sizeof(Item));
  ASSERT_TRUE(a != NULL);
  ASSERT_TRUE(parr_at(a, 0) == NULL);
  ASSERT_TRUE(parr_cat(a, 0) == NULL);

  parr_drop_swap(a, 0);
  ASSERT_TRUE(parr_len(a) == 0);

  parr_destroy(a);
}

static void test_index_bounds_nonempty(void) {
  PackedArray *a = parr_create(sizeof(Item));
  ASSERT_TRUE(a != NULL);

  CleanupCtx ctx = {0};
  parr_set_cleanup(a, cleanup_item, &ctx);

  Item *p = NULL;
  ASSERT_TRUE(parr_emplace(a, (void **)&p) == 0);
  p->id = 11;
  ASSERT_TRUE(parr_emplace(a, (void **)&p) == 1);
  p->id = 22;

  ASSERT_TRUE(parr_at(a, 0) != NULL);
  ASSERT_TRUE(parr_at(a, 1) != NULL);
  ASSERT_TRUE(parr_cat(a, 0) != NULL);
  ASSERT_TRUE(parr_cat(a, 1) != NULL);

  ASSERT_TRUE(parr_at(a, 2) == NULL);
  ASSERT_TRUE(parr_cat(a, 2) == NULL);

  // Out-of-range drop must be a no-op and must not trigger cleanup.
  parr_drop_swap(a, 2);
  ASSERT_TRUE(parr_len(a) == 2);
  ASSERT_TRUE(ctx.count == 0);

  parr_destroy(a);
}

static void test_emplace_failure(void) {
  PackedArray *a = parr_create_upper_bound(sizeof(Item), 1024);
  ASSERT_TRUE(a != NULL);

  Item *p = NULL;
  uint32_t idx = UINT32_MAX;
  size_t i = 0;
  for (;;) {
    idx = parr_emplace(a, (void **)&p);
    if (idx == UINT32_MAX)
      break;
    p->id = (int)i;
    i++;
  }
  ASSERT_TRUE(idx == UINT32_MAX);

  parr_destroy(a);
}

static void test_upper_bound(void) {
  size_t align = alignof(max_align_t);
  size_t stride = ((sizeof(Item) + align - 1) / align) * align;
  PackedArray *a = parr_create_upper_bound(sizeof(Item), stride * 2);
  ASSERT_TRUE(a != NULL);

  Item *p = NULL;
  uint32_t i0 = parr_emplace(a, (void **)&p);
  ASSERT_TRUE(i0 == 0);
  p->id = 1;

  uint32_t i1 = parr_emplace(a, (void **)&p);
  ASSERT_TRUE(i1 == 1);
  p->id = 2;

  uint32_t i2 = parr_emplace(a, (void **)&p);
  ASSERT_TRUE(i2 == UINT32_MAX);

  parr_destroy(a);
}

static void test_upper_bound_non_power_of_two_capacity(void) {
  size_t align = alignof(max_align_t);
  size_t stride = ((sizeof(Item) + align - 1) / align) * align;
  PackedArray *a = parr_create_upper_bound(sizeof(Item), stride * 3);
  ASSERT_TRUE(a != NULL);

  Item *p = NULL;
  ASSERT_TRUE(parr_emplace(a, (void **)&p) == 0);
  p->id = 1;
  ASSERT_TRUE(parr_emplace(a, (void **)&p) == 1);
  p->id = 2;
  ASSERT_TRUE(parr_emplace(a, (void **)&p) == 2);
  p->id = 3;

  ASSERT_TRUE(parr_emplace(a, (void **)&p) == UINT32_MAX);
  ASSERT_TRUE(parr_len(a) == 3);

  parr_destroy(a);
}

int main(void) {
  test_basic_emplace_and_at();
  test_invalid_inputs();
  test_index_bounds_nonempty();
  test_drop_swap();
  test_cleanup_drop_and_destroy();
  test_alignment();
  test_emplace_failure();
  test_upper_bound();
  test_upper_bound_non_power_of_two_capacity();

  fprintf(stderr, "OK: test_packed_array\n");
  return 0;
}
