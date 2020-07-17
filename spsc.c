#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdbool.h>
#include <assert.h>
#include <errno.h>

#define MAX_BURST_SIZE 64
#define RING_SIZE 512
#define RING_MASK (RING_SIZE - 1)
#define MIN(a, b) (a > b ? b : a)

typedef struct SPSCRing {
   volatile uint32_t p_head, p_tail;
   uint8_t pad1[56];
   volatile uint32_t c_head, c_tail;
   uint64_t pad2[56];
   uint32_t ring_size;
   uint32_t ring_mask;
   uint32_t ring_capacity;
} SPSCRing;

SPSCRing spscRing;

typedef struct WorkItem {
   uint32_t workId;
   void *data;
} WorkItem;

WorkItem *works;
volatile uint32_t workNum;
volatile bool terminate;

/*wait/wakeup mechnism for consumer and producer */
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

void ring_init(void)
{
   SPSCRing *ring = &spscRing;

   ring->p_head = ring->p_tail = 0;
   ring->c_head = ring->c_tail = 0;

   ring->ring_size = RING_SIZE;
   ring->ring_mask = RING_MASK;
   ring->ring_capacity = RING_MASK;

   works = malloc(RING_SIZE * sizeof (WorkItem));
   assert(works != NULL);

   return;
}

uint32_t enqueue_ring(WorkItem *items, uint32_t numItems)
{
   SPSCRing *ring = &spscRing;

   do {
      uint32_t i;
      uint32_t free_entries = ring->ring_size + ring->c_tail - ring->p_head;
      if (free_entries < numItems) {
         return 0;
      }

      printf("enque_ring num_items=%d free_entries=%d c_tail=%d p_head=%d\n",
             numItems, free_entries, ring->c_tail, ring->p_head);

      /* update head to reserve slots */
      uint32_t local_head = ring->p_head;
      ring->p_head = (ring->p_head + numItems);

      for (i = 0; i < numItems; i++) {
         works[(local_head + i) & ring->ring_mask] = items[i];
      }

      ring->p_tail = ring->p_head;
   } while (false);

   return numItems;
}


uint32_t dequeue_ring(WorkItem *items, uint32_t numItems)
{
   SPSCRing *ring = &spscRing;
   uint32_t numEntries;

   do {
      uint32_t filled_entries = ring->p_tail - ring->c_head;
      uint32_t i;

      if (filled_entries == 0) {
         return 0;
      }

      numEntries = MIN(numItems, filled_entries);

      printf("deque_ring num_items=%d filled_entries=%d p_tail=%d c_head=%d\n",
             numItems, filled_entries, ring->p_tail, ring->c_head);
             
      /* update consumer head */
      uint32_t local_head = ring->c_head;
      ring->c_head = (ring->c_head + numEntries);

      for (i = 0; i < numEntries; i++) {
         items[i] = works[(local_head + i) & ring->ring_mask];
      }

      ring->c_tail = ring->c_head;
   } while (false);

   return numEntries;
}

void *producer_thread(void *data)
{
   WorkItem *ws;

   ws = malloc(MAX_BURST_SIZE * sizeof(WorkItem));
   assert(ws != NULL);

   while (!terminate) {
      int i, ret;
      for (i = 0; i < MAX_BURST_SIZE; i++) {
         WorkItem *w = &ws[i];
         workNum++;

         w->workId = workNum;
         w->data = NULL;
      }
      do {
         ret = enqueue_ring(ws, MAX_BURST_SIZE);
         if (ret == 0) {
            printf("ring is full, sleeping... \n");
            usleep(10000);
         }
      } while (ret != MAX_BURST_SIZE && !terminate);
   }

   printf("num_produced:%d exiting producer thread \n", workNum);
   pthread_exit(0);     
}

void *consumer_thread(void *data)
{
   WorkItem *ws = malloc(MAX_BURST_SIZE * sizeof (WorkItem));
   uint32_t num_processed = 0;

   assert(ws != NULL);

   while (!terminate) {
      int ret, i;
      do {
         ret  = dequeue_ring(ws, MAX_BURST_SIZE);
         if (ret == 0) {
            printf("ring is empty. sleeping ...\n");
            usleep(10000);
         }
      } while (ret == 0 && !terminate);

      if (ret == 0) {
         break;
      }

      assert(ret <= MAX_BURST_SIZE);

      for (i = 0; i < ret; i++) {
         WorkItem *w = &ws[i];
         //printf("work_num = %d ", w->workId);
         num_processed++;
      }
   }

   printf("num_processed=%d, exiting consumer thread \n", num_processed);
   pthread_exit(0);
} 


void SigIntHandler(int sig)
{
   printf("Received sigint, requesting threads to exit\n");
   terminate = true;
}

int main()
{
   pthread_t threads[2];
   int ret;

   ring_init();

   signal(SIGINT, SigIntHandler);

   ret = pthread_create(&threads[0], NULL, producer_thread, NULL);
   if (ret < 0) {
      printf("pthread_create failed with errno:%d\n", errno);
      return -1;
   }

   ret = pthread_create(&threads[1], NULL, consumer_thread, NULL);
   if (ret < 0) {
      printf("pthread_create failed with errno:%d\n", errno);
      return -1;
   }

   pthread_join(threads[0], NULL);
   pthread_join(threads[1], NULL);

   return 0;
}
