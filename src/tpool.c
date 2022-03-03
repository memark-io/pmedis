#include "pmedis.h"

#define MAINTAINER_INTERVAL 60
#define WAITING_TASK_THRESHOLD 10
#define THREAD_NUM_CHANGE_STEP 5

typedef struct ThdPoolTask {
  void *(*func)(RedisModuleBlockedClient *, RedisModuleString **, int);
  RedisModuleBlockedClient *bc;
  RedisModuleString **argv;
  int argc;
} ThdPoolTask_t;

typedef struct ThdPool {
  pthread_mutex_t mtxPool;
  pthread_mutex_t mtxBusyThd;
  pthread_cond_t cndNotFull;
  pthread_cond_t cndNotEmpty;
  int nMinThd;
  int nMaxThd;
  int nLiveThd;
  int nBusyThd;
  int nToExitThd;
  int qHead;
  int qTail;
  int nTasks;
  int nMaxTask;
  int shutdown;
  pthread_t *tidWorkers;
  pthread_t tidMtner;
  ThdPoolTask_t *qTask;
} ThdPool_t;

ThdPool_t *thp;

int is_thd_alive(pthread_t tid) {
  int ret = pthread_kill(tid, 0);
  if (ret == ESRCH) {
    return 0;
  }
  return 1;
}

int thp_free(ThdPool_t *pool) {
  if (pool == NULL) {
    return -1;
  }

  if (pool->qTask) {
    free(pool->qTask);
  }

  if (pool->tidWorkers) {
    free(pool->tidWorkers);
    pthread_mutex_destroy(&(pool->mtxPool));
    pthread_mutex_destroy(&(pool->mtxBusyThd));
    pthread_cond_destroy(&(pool->cndNotEmpty));
    pthread_cond_destroy(&(pool->cndNotFull));
  }
  free(pool);
  pool = NULL;

  return 0;
}

int thp_destroy(ThdPool_t *pool) {
  int i;
  if (pool == NULL) {
    return -1;
  }

  pool->shutdown = 1;

  pthread_join(pool->tidMtner, NULL);

  for (i = 0; i < pool->nLiveThd; i++) {
    pthread_cond_broadcast(&(pool->cndNotEmpty));
  }
  for (i = 0; i < pool->nLiveThd; i++) {
    pthread_join(pool->tidWorkers[i], NULL);
  }
  thp_free(pool);

  return 0;
}

void *worker_thd(void *thp) {
  ThdPool_t *pool = (ThdPool_t *)thp;
  ThdPoolTask_t task;

  while (1) {
    pthread_mutex_lock(&(pool->mtxPool));

    while ((pool->nTasks == 0) && (!pool->shutdown)) {
      // thread is waiting for task
      pthread_cond_wait(&(pool->cndNotEmpty), &(pool->mtxPool));

      if (pool->nToExitThd > 0) {
        if (pool->nLiveThd > pool->nMinThd) {
          // thread is exiting
          pool->nLiveThd--;
          pool->nToExitThd--;
          pthread_mutex_unlock(&(pool->mtxPool));
          pthread_exit(NULL);
        }
      }
    }

    if (pool->shutdown == 1) {
      // thread is exiting
      pthread_mutex_unlock(&(pool->mtxPool));
      pthread_exit(NULL);
    }

    task.func = pool->qTask[pool->qHead].func;
    task.bc = pool->qTask[pool->qHead].bc;
    task.argv = pool->qTask[pool->qHead].argv;
    task.argc = pool->qTask[pool->qHead].argc;

    pool->qHead = (pool->qHead + 1) % pool->nMaxTask;
    pool->nTasks--;

    pthread_cond_broadcast(&(pool->cndNotFull));

    pthread_mutex_unlock(&(pool->mtxPool));

    pthread_mutex_lock(&(pool->mtxBusyThd));
    pool->nBusyThd++;
    pthread_mutex_unlock(&(pool->mtxBusyThd));

    (*(task.func))(task.bc, task.argv, task.argc);

    pthread_mutex_lock(&(pool->mtxBusyThd));
    pool->nBusyThd--;
    pthread_mutex_unlock(&(pool->mtxBusyThd));
  }

  return NULL;
}

void *maintainer_thd(void *thp) {
  ThdPool_t *pool = (ThdPool_t *)thp;
  int i;

  while (!pool->shutdown) {
    sleep(MAINTAINER_INTERVAL);

    pthread_mutex_lock(&(pool->mtxPool));
    int nTasks = pool->nTasks;
    int nLiveThd = pool->nLiveThd;
    pthread_mutex_unlock(&(pool->mtxPool));

    pthread_mutex_lock(&(pool->mtxBusyThd));
    int nBusyThd = pool->nBusyThd;
    pthread_mutex_unlock(&(pool->mtxBusyThd));

    if (nTasks >= WAITING_TASK_THRESHOLD && nLiveThd < pool->nMaxThd) {
      pthread_mutex_lock(&(pool->mtxPool));
      int add = 0;

      for (i = 0; i < pool->nMaxThd && add < THREAD_NUM_CHANGE_STEP &&
                  pool->nLiveThd < pool->nMaxThd;
           i++) {
        if (pool->tidWorkers[i] == 0 || !is_thd_alive(pool->tidWorkers[i])) {
          pthread_create(&(pool->tidWorkers[i]), NULL, worker_thd,
                         (void *)pool);
          add++;
          pool->nLiveThd++;
        }
      }

      pthread_mutex_unlock(&(pool->mtxPool));
    }

    if (nBusyThd * 2 < nLiveThd && nLiveThd > pool->nMinThd) {
      pthread_mutex_lock(&(pool->mtxPool));
      pool->nToExitThd = THREAD_NUM_CHANGE_STEP;
      pthread_mutex_unlock(&(pool->mtxPool));

      for (i = 0; i < THREAD_NUM_CHANGE_STEP; i++) {
        pthread_cond_signal(&(pool->cndNotEmpty));
      }
    }
  }

  return NULL;
}

ThdPool_t *thp_create(int nMinThd, int nMaxThd, int nMaxTask) {
  int i;
  ThdPool_t *pool = NULL;

  do {
    pool = (ThdPool_t *)malloc(sizeof(ThdPool_t));
    if (pool == NULL) {
      // malloc thp failed
      goto err_thp;
    }

    pool->nMinThd = nMinThd;
    pool->nMaxThd = nMaxThd;
    pool->nBusyThd = 0;
    pool->nLiveThd = nMinThd;
    pool->nTasks = 0;
    pool->nMaxTask = nMaxTask;
    pool->qHead = 0;
    pool->qTail = 0;
    pool->shutdown = 0;

    pool->tidWorkers = (pthread_t *)malloc(sizeof(pthread_t) * nMaxThd);
    if (pool->tidWorkers == NULL) {
      // malloc tidWorkers failed
      goto err_workers;
    }
    memset(pool->tidWorkers, 0, sizeof(pthread_t) * nMaxThd);

    pool->qTask = (ThdPoolTask_t *)malloc(sizeof(ThdPoolTask_t) * nMaxTask);
    if (pool->qTask == NULL) {
      // malloc qTask failed
      goto err_qtask;
    }

    if (pthread_mutex_init(&(pool->mtxPool), NULL) != 0 ||
        pthread_mutex_init(&(pool->mtxBusyThd), NULL) != 0 ||
        pthread_cond_init(&(pool->cndNotEmpty), NULL) != 0 ||
        pthread_cond_init(&(pool->cndNotFull), NULL) != 0) {
      // init lock and cond failed
      goto err_lockcond;
    }

    for (i = 0; i < nMinThd; i++) {
      pthread_create(&(pool->tidWorkers[i]), NULL, worker_thd, (void *)pool);
      // thread pool->tidWorkers[i] started
    }
    pthread_create(&(pool->tidMtner), NULL, maintainer_thd, (void *)pool);
    // maintainer_thd thread started

  } while (0);

  return pool;
err_lockcond:
  pthread_mutex_destroy(&(pool->mtxPool));
  pthread_mutex_destroy(&(pool->mtxBusyThd));
  pthread_cond_destroy(&(pool->cndNotEmpty));
  pthread_cond_destroy(&(pool->cndNotFull));
  free(pool->qTask);
err_qtask:
  free(pool->tidWorkers);
err_workers:
  free(pool);
err_thp:
  return NULL;
}

int thp_add(ThdPool_t *pool, void *func, RedisModuleBlockedClient *bc,
            RedisModuleString **argv, int argc) {
  pthread_mutex_lock(&(pool->mtxPool));

  while ((pool->nTasks == pool->nMaxTask) && (!pool->shutdown)) {
    // task queue is full, wait for running tasks to finish
    pthread_cond_wait(&(pool->cndNotEmpty), &(pool->mtxPool));
  }

  if (pool->shutdown) {
    pthread_mutex_unlock(&(pool->mtxPool));
    return 0;
  }

  pool->qTask[pool->qTail].func = func;
  pool->qTask[pool->qTail].bc = bc;
  pool->qTask[pool->qTail].argv = argv;
  pool->qTask[pool->qTail].argc = argc;
  pool->qTail = (pool->qTail + 1) % pool->nMaxTask;
  pool->nTasks++;

  pthread_cond_signal(&(pool->cndNotEmpty));
  pthread_mutex_unlock(&(pool->mtxPool));

  return 0;
}