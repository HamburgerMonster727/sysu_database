#ifndef __THREAD_POOL_H
#define __THREAD_POOL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#define CLOCK_REALTIME 0

typedef struct task{
    void *(*run)(void *arg1, void *arg2, void *arg3, void *arg4);   //需要执行的任务
    void *arg1;                  //参数
    void *arg2;
    void *arg3;    
    void *arg4;              
    struct task *next;          
}task;

typedef struct condition{
    pthread_mutex_t pmutex;
    pthread_cond_t pcond;
}condition;

typedef struct threadpool{
    condition condition_;   //状态量
    task *first;            //任务队列中第一个任务
    task *last;             //任务队列中最后一个任务
    int run_thread;         //运行线程数
    int space_thread;       //空闲线程数
    int max_thread;         //最大线程数
    int quit;               //是否退出标志
}threadpool;

//初始化
int condition_init(condition *cond);

//释放
int condition_destroy(condition *cond);

//线程池初始化
void threadpool_init(threadpool *pool, int threads);

//创建的线程执行
void *thread_run(void *arg);

//增加一个任务到线程池
void threadpool_add_task(threadpool *pool, void *(*run)(void *arg1, void *arg2, void *arg3, void *arg4), 
                        void *arg1, void *arg2, void *arg3, void *arg4);

//线程池销毁
void threadpool_destroy(threadpool *pool);

#endif // __THREAD_POOL_H