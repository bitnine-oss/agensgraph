/*
 * lib/port.c
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "libstatsinfo.h"

#ifndef WIN32

#include <sys/statfs.h>
#include <unistd.h>

bool
get_diskspace(const char *path, int64 *total, int64 *avail)
{
	struct statfs	fs;

	if (statfs(path, &fs) < 0)
		return false;

	*total = (int64) fs.f_bsize * (int64) fs.f_blocks;
	*avail = (int64) fs.f_bsize * (int64) fs.f_bfree;
	return true;
}

#else

#include <tlhelp32.h>

bool
get_diskspace(const char *path, int64 *total, int64 *avail)
{
	ULARGE_INTEGER	availBytes;
	ULARGE_INTEGER	totalBytes;
	ULARGE_INTEGER	freeBytes;

	if (!GetDiskFreeSpaceExA(path, &availBytes, &totalBytes, &freeBytes))
	{
		_dosmaperr(GetLastError());
		return false;
	}

	*total = (int64) totalBytes.QuadPart;
	*avail = (int64) availBytes.QuadPart;
	return true;
}

/*
 * CreateRestrictedProcess - See bin/pg_ctl.c.
 */

typedef BOOL (WINAPI * __CreateRestrictedToken) (HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);
typedef BOOL (WINAPI * __IsProcessInJob) (HANDLE, HANDLE, PBOOL);
typedef HANDLE (WINAPI * __CreateJobObjectA) (LPSECURITY_ATTRIBUTES, LPCSTR);
typedef BOOL (WINAPI * __SetInformationJobObject) (HANDLE, JOBOBJECTINFOCLASS, LPVOID, DWORD);
typedef BOOL (WINAPI * __AssignProcessToJobObject) (HANDLE, HANDLE);
typedef BOOL (WINAPI * __QueryInformationJobObject) (HANDLE, JOBOBJECTINFOCLASS, LPVOID, DWORD, LPDWORD);

/* Windows API define missing from MingW headers */
#define DISABLE_MAX_PRIVILEGE	0x1

static int
CreateRestrictedProcess(char *command, PROCESS_INFORMATION *processInfo, bool as_service)
{
	int				r;
	BOOL			b;
	STARTUPINFOA	si;
	HANDLE			origToken;
	HANDLE			restrictedToken;
	SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
	SID_AND_ATTRIBUTES dropSids[2];

	/* Functions loaded dynamically */
	__CreateRestrictedToken _CreateRestrictedToken = NULL;
	__IsProcessInJob _IsProcessInJob = NULL;
	__CreateJobObjectA _CreateJobObject = NULL;
	__SetInformationJobObject _SetInformationJobObject = NULL;
	__AssignProcessToJobObject _AssignProcessToJobObject = NULL;
	__QueryInformationJobObject _QueryInformationJobObject = NULL;
	HANDLE		Kernel32Handle;
	HANDLE		Advapi32Handle;

    SECURITY_ATTRIBUTES	sa = {0};
    HANDLE				hRead, hWrite;

	/* create unnamed pipes */
    sa.lpSecurityDescriptor = NULL;
    sa.nLength = sizeof(SECURITY_ATTRIBUTES);
    sa.bInheritHandle = TRUE;
    if (!CreatePipe(&hRead, &hWrite, &sa, 0))
		elog(ERROR, "cannot create pipes");

	DuplicateHandle(GetCurrentProcess(), hWrite,
					GetCurrentProcess(), NULL,
					0, FALSE, DUPLICATE_SAME_ACCESS);

	ZeroMemory(&si, sizeof(si));
	si.cb = sizeof(si);
	si.dwFlags = STARTF_USESTDHANDLES;
	si.hStdInput = hRead;
	si.hStdOutput = GetStdHandle(STD_OUTPUT_HANDLE);
	si.hStdError = GetStdHandle(STD_ERROR_HANDLE);

	Advapi32Handle = LoadLibraryA("ADVAPI32.DLL");
	if (Advapi32Handle != NULL)
	{
		_CreateRestrictedToken = (__CreateRestrictedToken) GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
	}

	if (_CreateRestrictedToken == NULL)
	{
		/*
		 * NT4 doesn't have CreateRestrictedToken, so just call ordinary
		 * CreateProcess
		 */
		if (Advapi32Handle != NULL)
			FreeLibrary(Advapi32Handle);
		b = CreateProcessA(NULL, command, NULL, NULL, FALSE, 0, NULL, NULL, &si, processInfo);
		goto done;
	}

	/* Open the current token to use as a base for the restricted one */
	if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken))
	{
		elog(WARNING, "could not open process token: %lu", GetLastError());
		return -1;
	}

	/* Allocate list of SIDs to remove */
	ZeroMemory(&dropSids, sizeof(dropSids));
	if (!AllocateAndInitializeSid(&NtAuthority, 2,
		 SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS, 0, 0, 0, 0, 0,
								  0, &dropSids[0].Sid) ||
		!AllocateAndInitializeSid(&NtAuthority, 2,
	SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_POWER_USERS, 0, 0, 0, 0, 0,
								  0, &dropSids[1].Sid))
	{
		elog(WARNING, "could not allocate SIDs: %lu", GetLastError());
		return -1;
	}

	b = _CreateRestrictedToken(origToken,
							   DISABLE_MAX_PRIVILEGE,
							   sizeof(dropSids) / sizeof(dropSids[0]),
							   dropSids,
							   0, NULL,
							   0, NULL,
							   &restrictedToken);

	FreeSid(dropSids[1].Sid);
	FreeSid(dropSids[0].Sid);
	CloseHandle(origToken);
	FreeLibrary(Advapi32Handle);

	if (!b)
	{
		elog(WARNING, "could not create restricted token: %lu", GetLastError());
		return -1;
	}

#ifndef __CYGWIN__
	AddUserToTokenDacl(restrictedToken);
#endif

	r = CreateProcessAsUserA(restrictedToken, NULL, command, NULL, NULL, TRUE, CREATE_SUSPENDED, NULL, NULL, &si, processInfo);

	Kernel32Handle = LoadLibraryA("KERNEL32.DLL");
	if (Kernel32Handle != NULL)
	{
		_IsProcessInJob = (__IsProcessInJob) GetProcAddress(Kernel32Handle, "IsProcessInJob");
		_CreateJobObject = (__CreateJobObjectA) GetProcAddress(Kernel32Handle, "CreateJobObjectA");
		_SetInformationJobObject = (__SetInformationJobObject) GetProcAddress(Kernel32Handle, "SetInformationJobObject");
		_AssignProcessToJobObject = (__AssignProcessToJobObject) GetProcAddress(Kernel32Handle, "AssignProcessToJobObject");
		_QueryInformationJobObject = (__QueryInformationJobObject) GetProcAddress(Kernel32Handle, "QueryInformationJobObject");
	}

	/* Verify that we found all functions */
	if (_IsProcessInJob == NULL || _CreateJobObject == NULL || _SetInformationJobObject == NULL || _AssignProcessToJobObject == NULL || _QueryInformationJobObject == NULL)
	{
		/*
		 * IsProcessInJob() is not available on < WinXP, so there is no need
		 * to log the error every time in that case
		 */
		OSVERSIONINFO osv;

		osv.dwOSVersionInfoSize = sizeof(osv);
		if (!GetVersionEx(&osv) ||		/* could not get version */
			(osv.dwMajorVersion == 5 && osv.dwMinorVersion > 0) ||		/* 5.1=xp, 5.2=2003, etc */
			osv.dwMajorVersion > 5)		/* anything newer should have the API */

			/*
			 * Log error if we can't get version, or if we're on WinXP/2003 or
			 * newer
			 */
			elog(WARNING, "could not locate all job object functions in system API");
	}
	else
	{
		BOOL		inJob;

		if (_IsProcessInJob(processInfo->hProcess, NULL, &inJob))
		{
			if (!inJob)
			{
				/*
				 * Job objects are working, and the new process isn't in one,
				 * so we can create one safely. If any problems show up when
				 * setting it, we're going to ignore them.
				 */
				HANDLE		job;
				char		jobname[128];

				sprintf(jobname, "PostgreSQL_%lu", processInfo->dwProcessId);

				job = _CreateJobObject(NULL, jobname);
				if (job)
				{
					JOBOBJECT_BASIC_LIMIT_INFORMATION basicLimit;
					JOBOBJECT_BASIC_UI_RESTRICTIONS uiRestrictions;
					JOBOBJECT_SECURITY_LIMIT_INFORMATION securityLimit;
					OSVERSIONINFO osv;

					ZeroMemory(&basicLimit, sizeof(basicLimit));
					ZeroMemory(&uiRestrictions, sizeof(uiRestrictions));
					ZeroMemory(&securityLimit, sizeof(securityLimit));

					basicLimit.LimitFlags = JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION | JOB_OBJECT_LIMIT_PRIORITY_CLASS;
					basicLimit.PriorityClass = NORMAL_PRIORITY_CLASS;
					_SetInformationJobObject(job, JobObjectBasicLimitInformation, &basicLimit, sizeof(basicLimit));

					uiRestrictions.UIRestrictionsClass = JOB_OBJECT_UILIMIT_DESKTOP | JOB_OBJECT_UILIMIT_DISPLAYSETTINGS |
						JOB_OBJECT_UILIMIT_EXITWINDOWS | JOB_OBJECT_UILIMIT_READCLIPBOARD |
						JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS | JOB_OBJECT_UILIMIT_WRITECLIPBOARD;

					if (as_service)
					{
						osv.dwOSVersionInfoSize = sizeof(osv);
						if (!GetVersionEx(&osv) ||
							osv.dwMajorVersion < 6 ||
						(osv.dwMajorVersion == 6 && osv.dwMinorVersion == 0))
						{
							/*
							 * On Windows 7 (and presumably later),
							 * JOB_OBJECT_UILIMIT_HANDLES prevents us from
							 * starting as a service. So we only enable it on
							 * Vista and earlier (version <= 6.0)
							 */
							uiRestrictions.UIRestrictionsClass |= JOB_OBJECT_UILIMIT_HANDLES;
						}
					}
					_SetInformationJobObject(job, JobObjectBasicUIRestrictions, &uiRestrictions, sizeof(uiRestrictions));

					securityLimit.SecurityLimitFlags = JOB_OBJECT_SECURITY_NO_ADMIN | JOB_OBJECT_SECURITY_ONLY_TOKEN;
					securityLimit.JobToken = restrictedToken;
					_SetInformationJobObject(job, JobObjectSecurityLimitInformation, &securityLimit, sizeof(securityLimit));

					_AssignProcessToJobObject(job, processInfo->hProcess);
				}
			}
		}
	}


	CloseHandle(restrictedToken);

	ResumeThread(processInfo->hThread);

	FreeLibrary(Kernel32Handle);

done:
	/*
	 * We intentionally don't close the job object handle, because we want the
	 * object to live on until this program shuts down.
	 */
	if (r)
	{
		int		fd;
		CloseHandle(hRead);
		if ((fd = _open_osfhandle((intptr_t) hWrite, O_WRONLY | O_TEXT)) != -1)
			return fd;
		else
			CloseHandle(hWrite);
	}
	else
	{
		CloseHandle(hRead);
		CloseHandle(hWrite);
	}
	return -1;
}

pid_t
getppid(void)
{
	PROCESSENTRY32	pe = { sizeof(PROCESSENTRY32) };
	DWORD			mypid;
	HANDLE			h;

	mypid = GetCurrentProcessId();
	h = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
	if (Process32First(h, &pe))
	{
		do 
		{
			if (pe.th32ProcessID == mypid)
			{
				CloseHandle(h);
				return (pid_t) pe.th32ParentProcessID;
			}
		} while (Process32Next(h, &pe));
	}

	CloseHandle(h);
	elog(ERROR, "postmaster process not found");
	return 0;	/* keep compiler quiet */
}

#endif	/* WIN32 */

/*
 * start command asynchronously.
 */
pid_t
forkexec(const char *cmd, int *outStdin)
{
	pid_t	pid;

	*outStdin = -1;

#ifndef WIN32
	{
		int		fd[2];

		if (pipe(fd) < 0 || (pid = fork()) < 0)
			return 0;

		if (pid == 0)
		{
			/* child process */
			if (close(fd[1]) < 0 || dup2(fd[0], STDIN_FILENO) < 0 ||
				execl("/bin/sh", "sh", "-c", cmd, NULL) < 0)
			{
				elog(LOG, "pg_statsinfo(): could not execute background process: %s",
					 strerror(errno));
				exit(1);
			}
		}

		close(fd[0]);
		*outStdin = fd[1];
	}
#else
	{
		PROCESS_INFORMATION pi;

		*outStdin = CreateRestrictedProcess((char *) cmd, &pi, false);
		if (*outStdin)
			pid = (pid_t) pi.dwProcessId;
		else
		{
			_dosmaperr(GetLastError());
			pid = 0;
		}
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
	}
#endif

	return pid;
}
