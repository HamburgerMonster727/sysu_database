#include "thread_pool.h"

//初始化
int condition_init(condition *cond){
    int status;
    if((status = pthread_mutex_init(&cond->pmutex, NULL)))
        return status;
    if((status = pthread_cond_init(&cond->pcond, NULL)))
        return status;
    return 0;
}

//释放
int condition_destroy(condition *cond){
    int status;
    if((status = pthread_mutex_destroy(&cond->pmutex)))
        return status;
    if((status = pthread_cond_destroy(&cond->pcond)))
        return status;
    return 0;
}

//线程池初始化
void threadpool_init(threadpool *pool, int threads){
    condition_init(&pool->condition_);
    pool->first = NULL;
    pool->last = NULL;
    pool->run_thread = 0;
    pool->space_thread = 0;
    pool->max_thread = threads;
    pool->quit = 0;
}

//创建的线程执行
void *thread_run(void *arg){
    struct timespec abstime;
    int timeout;
    //printf("thread %lu is starting\n", pthread_self());
    threadpool *pool = (threadpool *)arg;
    while(1){
        timeout = 0;
        //访问线程池之前需要加锁
        pthread_mutex_lock(&(pool->condition_).pmutex);
        pool->space_thread++;
        //等待队列有任务到来
        while(pool->first == NULL && !pool->quit){
            //printf("thread %lu is waiting\n", pthread_self());
            //获取从当前时间，并加上等待时间，设置进程的超时睡眠时间
            clock_gettime(CLOCK_REALTIME, &abstime);
            abstime.tv_sec += 2;
            int status;
            //该函数会解锁，允许其他线程访问，当被唤醒时，加锁
            status = pthread_cond_timedwait(&(pool->condition_).pcond, &(pool->condition_).pmutex, &abstime); 
            if(status == ETIMEDOUT){
                //printf("thread %lu wait timed out\n", pthread_self());
                timeout = 1;
                break;
            }
        }

        pool->space_thread--;

        if(pool->first != NULL){
            task *t = pool->first;
            pool->first = t->next;
            //由于任务执行需要消耗时间，先解锁让其他线程访问线程池
            pthread_mutex_unlock(&(pool->condition_).pmutex);
            //执行任务
            t->run(t->arg1, t->arg2, t->arg3, t->arg4);
            //执行完任务释放内存
            free(t);
            //重新加锁
            pthread_mutex_lock(&(pool->condition_).pmutex);
        }

        //退出线程池
        if(pool->quit && pool->first == NULL){
            pool->run_thread--;
            //若线程池中没有线程，通知等待线程全部任务已经完成
            if(pool->run_thread == 0){
                pthread_cond_signal(&(pool->condition_).pcond);
            }
            pthread_mutex_unlock(&(pool->condition_).pmutex);
            break;
        }
        //超时，退出线程池
        if(timeout == 1){
            pool->run_thread--;
            pthread_mutex_unlock(&(pool->condition_).pmutex);
            break;
        }
        pthread_mutex_unlock(&(pool->condition_).pmutex);
    }

    //printf("thread %lu is exiting\n", pthread_self());
    return NULL;
}

//增加一个任务到线程池
void threadpool_add_task(threadpool *pool, void *(*run)(void *arg1, void *arg2, void *arg3, void *arg4), 
                        void *arg1, void *arg2, void *arg3, void *arg4){
    task *newtask = (task *)malloc(sizeof(task));
    newtask->run = run;
    newtask->arg1 = arg1;
    newtask->arg2 = arg2;
    newtask->arg3 = arg3;
    newtask->arg4 = arg4;
    newtask->next = NULL;

    //线程池的状态被多个线程共享，操作前需要加锁
    pthread_mutex_lock(&(pool->condition_).pmutex);

    if(pool->first == NULL){
        pool->first = newtask;
    }
    else{
        pool->last->next = newtask;
    }
    pool->last = newtask;  

    //线程池中有线程空闲，唤醒
    if(pool->space_thread > 0){
        pthread_cond_signal(&(pool->condition_).pcond);
    }
    //当前线程池中线程个数没有达到设定的最大值，创建一个新的线程
    else if(pool->run_thread < pool->max_thread){
        pthread_t tid;
        pthread_create(&tid, NULL, thread_run, pool);
        pool->run_thread++;
    }
    //结束，访问
    pthread_mutex_unlock(&(pool->condition_).pmutex);
}

//线程池销毁
void threadpool_destroy(threadpool *pool){
    if(pool->quit == 1){
        return;
    }
    //加锁
    pthread_mutex_lock(&(pool->condition_).pmutex);
    pool->quit = 1;
    if(pool->run_thread > 0){
        //对于等待的线程，发送信号唤醒
        if(pool->space_thread > 0){
            pthread_cond_broadcast(&(pool->condition_).pcond);
        }
        //正在执行任务的线程，等待他们结束任务
        while(pool->run_thread){
            pthread_cond_wait(&(pool->condition_).pcond, &(pool->condition_).pmutex);
        }
    }
    pthread_mutex_unlock(&(pool->condition_).pmutex);
    condition_destroy(&pool->condition_);
}