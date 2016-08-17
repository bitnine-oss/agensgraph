/*-------------------------------------------------------------------------
 *
 * pgut-pthread.c: Portable pthread implementation and support functions.
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "pgut.h"
#include "pgut-pthread.h"

void
pgut_mutex_lock(pthread_mutex_t *mutex)
{
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		errno = pthread_mutex_lock(mutex);
		if (errno == 0)
			break;
		else if (errno != EINTR)
			ereport(PANIC,
				(errcode_errno(),
				 errmsg("pthread_mutex_lock: ")));
	}
}

void
pgut_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		errno = pthread_cond_wait(cond, mutex);
		if (errno == 0)
			break;
		else if (errno != EINTR)
			ereport(PANIC,
				(errcode_errno(),
				 errmsg("pthread_cond_wait: ")));
	}
}

#ifdef WIN32

typedef struct win32_pthread
{
	HANDLE		handle;
	void	   *(*routine)(void *);
	void	   *arg;
	void	   *result;
} win32_pthread;

/* allocated on thread local storage */
__declspec(thread) static pthread_t		self_thread;

static unsigned __stdcall
win32_pthread_run(void *arg)
{
	win32_pthread *th = (win32_pthread *) arg;

	self_thread = th;
	th->result = th->routine(th->arg);

	return 0;
}

#define DETACHSTATE_MASK	(PTHREAD_CREATE_JOINABLE | PTHREAD_CREATE_DETACHED)

static int
maperr(void)
{
	_dosmaperr(GetLastError());
	return errno;
}

pthread_t
pthread_self(void)
{
	return self_thread;
}

int
pthread_create(pthread_t *thread,
			   pthread_attr_t *attr,
			   void * (*start_routine)(void *),
			   void *arg)
{
	int				save_errno;
	win32_pthread   *th;

	if ((th = malloc(sizeof(win32_pthread))) == NULL)
		return errno = ENOMEM;
	th->routine = start_routine;
	th->arg = arg;
	th->result = NULL;

	th->handle = (HANDLE) _beginthreadex(NULL, 0, win32_pthread_run, th, 0, NULL);
	if (th->handle == NULL)
	{
		save_errno = errno;
		free(th);
		return save_errno;
	}

	if (attr && (*attr & DETACHSTATE_MASK) == PTHREAD_CREATE_DETACHED)
	{
		CloseHandle(th->handle);
		th->handle = NULL;
	}

	*thread = th;
	return 0;
}

int
pthread_detach(pthread_t th)
{
	if (th == NULL)
		return 0;

	if (th->handle != NULL && !CloseHandle(th->handle))
		return maperr();

	free(th);
	return 0;
}

int
pthread_join(pthread_t th, void **thread_return)
{
	if (th == NULL || th->handle == NULL)
		return errno = EINVAL;

	if (WaitForSingleObject(th->handle, INFINITE) != WAIT_OBJECT_0)
		return maperr();

	if (thread_return)
		*thread_return = th->result;

	CloseHandle(th->handle);
	free(th);
	return 0;
}

int
pthread_attr_init(pthread_attr_t *attr)
{
	*attr = 0;
	return 0;
}

int
pthread_attr_destroy(pthread_attr_t *attr)
{
	/* do nothing */
	return 0;
}

int
pthread_attr_setdetachstate(pthread_attr_t *attr, int detachstate)
{
	*attr = ((*attr & ~DETACHSTATE_MASK) | detachstate);
	return 0;
}

int
pthread_attr_getdetachstate(const pthread_attr_t *attr, int *detachstate)
{
	*detachstate = (*attr & DETACHSTATE_MASK);
	return 0;
}

int
pthread_mutex_init(pthread_mutex_t *mutex, void *attr)
{
	InitializeCriticalSection(mutex);
	return 0;
}

int
pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	/* do nothing */
	return 0;
}

int
pthread_mutex_lock(pthread_mutex_t *mutex)
{
	EnterCriticalSection(mutex);
	return 0;
}

int
pthread_mutex_unlock(pthread_mutex_t *mutex)
{
	LeaveCriticalSection(mutex);
	return 0;
}

int
pthread_cond_init(pthread_cond_t *cond, pthread_condattr_t *cond_attr)
{
	if (cond_attr != NULL)
		return errno = EINVAL;	/* cond_attr is not supported for now. */
	if ((*cond = CreateEvent(NULL, FALSE, FALSE, NULL)) == NULL)
		return maperr();
	return 0;
}

int
pthread_cond_signal(pthread_cond_t *cond)
{
	/* single wakeup is not supported for now. */
	return pthread_cond_broadcast(cond);
}

int
pthread_cond_broadcast(pthread_cond_t *cond)
{
	if (!SetEvent(*cond))
		return maperr();
	return 0;
}

int
pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
	int	ret;

	if ((ret = pthread_mutex_unlock(mutex)) != 0)
		return ret;
	if (WaitForSingleObject(*cond, INFINITE) != WAIT_OBJECT_0)
		return maperr();
	if ((ret = pthread_mutex_lock(mutex)) != 0)
		return ret;
	return 0;
}

int
pthread_cond_destroy(pthread_cond_t *cond)
{
	if (cond && *cond && !CloseHandle(*cond))
		return maperr();
	return 0;
}

int
pthread_key_create(pthread_key_t *key, void (*destr_function) (void *))
{
	Assert(destr_function == NULL);	/* not supported */

	if ((*key = TlsAlloc()) != 0xFFFFFFFF)
		return 0;
	else
		return maperr();
}

int
pthread_key_delete(pthread_key_t key)
{
	if (TlsFree(key))
		return 0;
	else
		return maperr();
}

int
pthread_setspecific(pthread_key_t key, const void *pointer)
{
	if (TlsSetValue(key, (void *) pointer))
		return 0;
	else
		return maperr();
}

void *
pthread_getspecific(pthread_key_t key)
{
	return TlsGetValue(key);
}

#endif
