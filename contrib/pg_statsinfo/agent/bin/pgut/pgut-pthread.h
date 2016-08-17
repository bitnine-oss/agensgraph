/*-------------------------------------------------------------------------
 *
 * pgut-pthread.h
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_PTHREAD_H
#define PGUT_PTHREAD_H

#ifndef WIN32

#include <pthread.h>

#else

struct win32_pthread;

typedef struct win32_pthread   *pthread_t;
typedef int						pthread_attr_t;
typedef DWORD					pthread_key_t;
typedef CRITICAL_SECTION		pthread_mutex_t;
typedef void				   *pthread_cond_t;
typedef int						pthread_condattr_t;
typedef DWORD					pthread_key_t;

#define PTHREAD_CREATE_JOINABLE		0x0
#define PTHREAD_CREATE_DETACHED		0x1

extern pthread_t pthread_self(void);
extern int pthread_create(pthread_t *thread, pthread_attr_t *attr, void * (*start_routine)(void *), void *arg);
extern int pthread_detach(pthread_t th);
extern int pthread_join(pthread_t th, void **thread_return);
extern int pthread_attr_init(pthread_attr_t *attr);
extern int pthread_attr_destroy(pthread_attr_t *attr);
extern int pthread_attr_setdetachstate(pthread_attr_t *attr, int detachstate);
extern int pthread_attr_getdetachstate(const pthread_attr_t *attr, int *detachstate);

extern int pthread_mutex_init(pthread_mutex_t *mutex, void *attr);
extern int pthread_mutex_destroy(pthread_mutex_t *mutex);
extern int pthread_mutex_lock(pthread_mutex_t *mutex);
extern int pthread_mutex_unlock(pthread_mutex_t *mutex);

extern int pthread_cond_init(pthread_cond_t *cond, pthread_condattr_t *cond_attr);
extern int pthread_cond_signal(pthread_cond_t *cond);
extern int pthread_cond_broadcast(pthread_cond_t *cond);
extern int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);
extern int pthread_cond_destroy(pthread_cond_t *cond);

extern int pthread_key_create(pthread_key_t *key, void (*destr_function) (void *));
extern int pthread_key_delete(pthread_key_t key);
extern int pthread_setspecific(pthread_key_t key, const void *pointer);
extern void * pthread_getspecific(pthread_key_t key);

#endif

extern void pgut_mutex_lock(pthread_mutex_t *mutex);
extern void pgut_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);

#endif   /* PGUT_PTHREAD_H */
