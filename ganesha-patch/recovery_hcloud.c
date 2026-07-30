// SPDX-License-Identifier: LGPL-3.0-or-later
/*
 * hcloud recovery backend for nfs-ganesha.
 *
 * Derived from the Longhorn recovery backend (recovery_longhorn.c) in
 * https://github.com/rancher/nfs-ganesha, originally developed by the
 * Longhorn authors (Copyright the Longhorn/Rancher contributors).
 *
 * Modifications for hcloud-csi-rwx (Copyright 2026 Mathias Rhein):
 *   - renamed longhorn -> hcloud, backend URL configurable via
 *     HCLOUD_RECOVERY_BACKEND_URL
 *   - ported to the nfs-ganesha V12.0 recovery backend API
 *     (nfs4_add_clid_entry, changed callback signatures)
 *   - hardened HTTP handling (timeouts, NUL termination, error paths
 *     instead of assert(), no trailing NUL in request bodies)
 *   - optional bearer-token authentication (RECOVERY_BACKEND_TOKEN)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */

#include "config.h"
#include "log.h"
#include "nfs_core.h"
#include "nfs4.h"
#include "sal_functions.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include "bsd-base64.h"
#include "client_mgr.h"
#include "fsal.h"
#include "common_utils.h"
#include <libgen.h>
#include <curl/curl.h>
#include <json-c/json.h>

#define VERSION_BYTES 8
#define URL_MAX       2048
#define PAYLOAD_MAX   2048
#define AUTH_HEADER_MAX 512
#define HCLOUD_RECOVERY_BACKEND_URL "http://hcloud-csi-rwx-recovery-backend:9503/v1/recoverybackend"

static char recov_version[NAME_MAX];
static pthread_rwlock_t recov_lock = PTHREAD_RWLOCK_INITIALIZER;
/* Base URL of the recovery backend; overridable via env in recov_init. */
static char recov_url[URL_MAX] = HCLOUD_RECOVERY_BACKEND_URL;
/* "Authorization: Bearer <token>" if RECOVERY_BACKEND_TOKEN is set. */
static char auth_header[AUTH_HEADER_MAX];

typedef enum {
	HTTP_GET = 0,
	HTTP_POST,
	HTTP_PUT,
	HTTP_DELETE,
} HTTP_METHOD;

struct http_result {
	void *memory;
	size_t size;
};

static char *generate_random_string(const int len)
{
	static const char alphanum[] =
		"0123456789"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	char *buf;

	buf = malloc(len + 1);
	if (!buf) {
		return NULL;
	}

	for (int i = 0; i < len; ++i) {
		buf[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
	}

	buf[len] = '\0';

	return buf;
}

static size_t callback_write_result(void *contents, size_t size, size_t nmemb, void *userp)
{
	char *buf = NULL;
	size_t real_size = size * nmemb;

	if (contents != NULL && userp) {
		struct http_result *mem = (struct http_result *) userp;
		buf = realloc(mem->memory, mem->size + real_size + 1);
		if (buf) {
			mem->memory = buf;
			memcpy(&(((unsigned char *)mem->memory)[mem->size]), contents, real_size);
			mem->size += real_size;
			/* keep the buffer usable as a C string */
			((char *)mem->memory)[mem->size] = '\0';
			return real_size;
		}
	}
	return 0;
}

static int http_call(HTTP_METHOD method, const char *url, char *payload, size_t payload_size, char **output, size_t *output_size)
{
	int result = -1;
	struct http_result buffer = {.memory = NULL, .size = 0};
	CURL *handle = NULL;
	CURLcode curl_result = 0;
	struct curl_slist *curl_headers = NULL;
	long http_code = 0;

	if (method < HTTP_GET || method > HTTP_DELETE) {
		LogEvent(COMPONENT_CLIENTID, "Invalid method: %d", method);
		goto error;
	}

	if (!url) {
		LogEvent(COMPONENT_CLIENTID, "url is NULL");
		goto error;
	}

	/* Initialize CURL handle */
	handle = curl_easy_init();
	if (!handle) {
		LogEvent(COMPONENT_CLIENTID, "Failed to initialize CURL");
		goto error;
	}

	/* Set CURL options */
	curl_result = curl_easy_setopt(handle, CURLOPT_URL, url);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	curl_result = curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	curl_result = curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, callback_write_result);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	curl_result = curl_easy_setopt(handle, CURLOPT_WRITEDATA, (void *)&buffer);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	curl_result = curl_easy_setopt(handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	/* Never block ganesha indefinitely on a stuck backend. */
	curl_result = curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT, 10L);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	curl_result = curl_easy_setopt(handle, CURLOPT_TIMEOUT, 30L);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	switch (method) {
		case HTTP_GET:
			curl_result = curl_easy_setopt(handle, CURLOPT_HTTPGET, 1L);
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}
			break;
		case HTTP_POST:
			curl_result = curl_easy_setopt(handle, CURLOPT_POST, 1L);
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}

			curl_result = curl_easy_setopt(handle, CURLOPT_POSTFIELDS, payload);
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}

			curl_result = curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, payload_size);
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}

			break;
		case HTTP_PUT:
			curl_result = curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "PUT");
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}

			curl_result = curl_easy_setopt(handle, CURLOPT_POSTFIELDS, payload);
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}

			curl_result = curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, payload_size);
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}

			break;
		case HTTP_DELETE:
			curl_result = curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "DELETE");
			if (curl_result != CURLE_OK) {
				LogEvent(COMPONENT_CLIENTID, "Failed to set CURL option: %s", curl_easy_strerror(curl_result));
				goto error;
			}
	}

	/* Set HTTP headers */
	curl_headers = curl_slist_append(curl_headers, "Accept: application/json");
	if (!curl_headers) {
		LogEvent(COMPONENT_CLIENTID, "Failed to construct CURL headers");
		goto error;
	}

	curl_headers = curl_slist_append(curl_headers, "Content-Type: application/json; charset=utf-8");
	if (!curl_headers) {
		LogEvent(COMPONENT_CLIENTID, "Failed to construct CURL headers");
		goto error;
	}

	curl_headers = curl_slist_append(curl_headers, "Connection: close");
	if (!curl_headers) {
		LogEvent(COMPONENT_CLIENTID, "Failed to construct CURL headers");
		goto error;
	}

	if (auth_header[0] != '\0') {
		curl_headers = curl_slist_append(curl_headers, auth_header);
		if (!curl_headers) {
			LogEvent(COMPONENT_CLIENTID, "Failed to construct CURL headers");
			goto error;
		}
	}

	curl_result = curl_easy_setopt(handle, CURLOPT_HTTPHEADER, curl_headers);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to set CURL headers: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	/* Make HTTP request */
	curl_result = curl_easy_perform(handle);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to perform CURL operation: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	curl_result = curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &http_code);
	if (curl_result != CURLE_OK) {
		LogEvent(COMPONENT_CLIENTID, "Failed to perform CURL operation: %s", curl_easy_strerror(curl_result));
		goto error;
	}

	if (http_code != 200) {
		LogEvent(COMPONENT_CLIENTID, "HTTP error: %ld (url=%s, payload=%s)",
			 http_code, url, payload ? payload : "(none)");
		goto error;
	}

	*output = buffer.memory;
	*output_size = buffer.size;
	result = 0;
error:
	if (result != 0) {
		if (buffer.memory != NULL) {
			free(buffer.memory);
			buffer.memory = NULL;
			*output = NULL;
			*output_size = 0;
		}
	}
	if (curl_headers != NULL)
		curl_slist_free_all(curl_headers);
	if (handle != NULL)
		curl_easy_cleanup(handle);

	return result;
}

/**
 * @brief convert clientid opaque bytes as a hex string for mkdir purpose.
 *
 * @param[in,out] dspbuf The buffer.
 * @param[in]     value  The bytes to display
 * @param[in]     len    The number of bytes to display
 *
 * @return the bytes remaining in the buffer.
 *
 */
static int hcloud_convert_opaque_value_max_for_dir(struct display_buffer *dspbuf,
					       void *value,
					       int len,
					       int max)
{
	unsigned int i = 0;
	int          b_left = display_start(dspbuf);
	int          cpy = len;

	if (b_left <= 0)
		return 0;

	/* Check that the length is ok
	 * If the value is empty, display EMPTY value. */
	if (len <= 0 || len > max)
		return 0;

	/* If the value is NULL, display NULL value. */
	if (value == NULL)
		return 0;

	/* Determine if the value is entirely printable characters, */
	/* and it contains no slash character (reserved for filename) */
	for (i = 0; i < len; i++)
		if ((!isprint(((char *)value)[i])) ||
		    (((char *)value)[i] == '/'))
			break;

	if (i == len) {
		/* Entirely printable character, so we will just copy the
		 * characters into the buffer (to the extent there is room
		 * for them).
		 */
		b_left = display_len_cat(dspbuf, value, cpy);
	} else {
		b_left = display_opaque_bytes(dspbuf, value, cpy);
	}

	if (b_left <= 0)
		return 0;

	return b_left;
}

/**
 * @brief generate a name that identifies this client
 *
 * This name will be used to know that a client was talking to the
 * server before a restart so that it will be allowed to do reclaims
 * during grace period.
 *
 * @param[in] clientid Client record
 */
static void hcloud_create_clid_name(nfs_client_id_t *clientid)
{
	nfs_client_record_t *cl_rec = clientid->cid_client_record;
	const char *str_client_addr = "(unknown)";
	char cidstr[PATH_MAX] = { 0, };
	struct display_buffer dspbuf = {sizeof(cidstr), cidstr, cidstr};
	char cidstr_lenx[5];
	int total_size, cidstr_lenx_len, cidstr_len, str_client_addr_len;

	/* get the caller's IP addr */
	if (clientid->gsh_client != NULL)
		str_client_addr = clientid->gsh_client->hostaddr_str;

	if (hcloud_convert_opaque_value_max_for_dir(&dspbuf,
						cl_rec->cr_client_val,
						cl_rec->cr_client_val_len,
						PATH_MAX) > 0) {
		cidstr_len = strlen(cidstr);
		str_client_addr_len = strlen(str_client_addr);

		/* hcloud_convert_opaque_value_max_for_dir does not prefix
		 * the "(<length>:". So we need to do it here */
		cidstr_lenx_len = snprintf(cidstr_lenx, sizeof(cidstr_lenx),
					   "%d", cidstr_len);

		if (unlikely(cidstr_lenx_len >= sizeof(cidstr_lenx) ||
			     cidstr_lenx_len < 0)) {
			/* cidrstr can at most be PATH_MAX or 1024, so at most
			 * 4 characters plus NUL are necessary, so we won't
			 * overrun, nor can we get a -1 with EOVERFLOW or EINVAL
			 */
			LogFatal(COMPONENT_CLIENTID,
				 "snprintf returned unexpected %d",
				 cidstr_lenx_len);
		}

		total_size = cidstr_len + str_client_addr_len + 5 +
			     cidstr_lenx_len;

		/* hold both long form clientid and IP */
		clientid->cid_recov_tag = gsh_malloc(total_size);

		/* Can't overrun and shouldn't return EOVERFLOW or EINVAL */
		(void) snprintf(clientid->cid_recov_tag, total_size,
				"%s:%s",
				cidstr_lenx, cidstr);
	}

	LogDebug(COMPONENT_CLIENTID, "Created client name [%s]",
		 clientid->cid_recov_tag);
}

static int hcloud_recov_init(void)
{
	char host[NI_MAXHOST];
	char payload[PAYLOAD_MAX];
	char *response = NULL;
	size_t response_size = 0;
	char *version = NULL;
	const char *env_url = NULL;
	const char *env_token = NULL;
	int err = 0;
	int res = 0;

	err = gethostname(host, sizeof(host));
	if (err) {
		LogEvent(COMPONENT_CLIENTID,
				 "Failed to gethostname: %s (%d)",
				 strerror(errno), errno);
		return -errno;
	}

	env_url = getenv("HCLOUD_RECOVERY_BACKEND_URL");
	if (env_url != NULL && env_url[0] != '\0') {
		if (snprintf(recov_url, sizeof(recov_url), "%s", env_url) >=
		    (int)sizeof(recov_url)) {
			LogEvent(COMPONENT_CLIENTID,
				 "HCLOUD_RECOVERY_BACKEND_URL too long");
			return -EINVAL;
		}
	}

	env_token = getenv("RECOVERY_BACKEND_TOKEN");
	if (env_token != NULL && env_token[0] != '\0') {
		if (snprintf(auth_header, sizeof(auth_header),
			     "Authorization: Bearer %s", env_token) >=
		    (int)sizeof(auth_header)) {
			LogEvent(COMPONENT_CLIENTID,
				 "RECOVERY_BACKEND_TOKEN too long");
			return -EINVAL;
		}
	}

	LogEvent(COMPONENT_CLIENTID, "Initialize recovery backend '%s' (url=%s)",
		 host, recov_url);

	version = generate_random_string(VERSION_BYTES);
	if (version == NULL) {
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to allocate recovery version string");
		return -ENOMEM;
	}

	memcpy(recov_version, version, VERSION_BYTES + 1);

	snprintf(payload, sizeof(payload), "{\"hostname\": \"%s\", \"version\": \"%s\"}",
		host, recov_version);

	free(version);
	version = NULL;

	PTHREAD_RWLOCK_wrlock(&recov_lock);
	res = http_call(HTTP_POST, recov_url,
		payload, strlen(payload),
		&response, &response_size);
	PTHREAD_RWLOCK_unlock(&recov_lock);
	if (res != 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to initialize recovery backend. "
			"HTTP call error: res=%d (%s)", res,
			response ? response : "(none)");
		free(response);
		return -EINVAL;
	}
	free(response);
	return 0;
}

static void hcloud_recov_end_grace(void)
{
	char host[NI_MAXHOST];
	char url[URL_MAX];
	char payload[PAYLOAD_MAX];
	char *response = NULL;
	size_t response_size = 0;
	int err = 0;
	int res = 0;

	err = gethostname(host, sizeof(host));
	if (err) {
		LogEvent(COMPONENT_CLIENTID,
				 "Failed to gethostname: %s (%d)",
				 strerror(errno), errno);
		return;
	}

	LogEvent(COMPONENT_CLIENTID,
			 "End grace for recovery backend '%s' version %s",
			 host, recov_version);

	snprintf(url, sizeof(url), "%s/%s", recov_url, host);
	snprintf(payload, sizeof(payload), "{\"version\": \"%s\"}", recov_version);

	PTHREAD_RWLOCK_wrlock(&recov_lock);
	res = http_call(HTTP_PUT, url, payload, strlen(payload), &response, &response_size);
	PTHREAD_RWLOCK_unlock(&recov_lock);
	if (res != 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to end grace period in recovery backend. "
			"HTTP call error: res=%d (%s)", res,
			response ? response : "(none)");
	}
	free(response);
}

static void hcloud_add_clid(nfs_client_id_t *clientid)
{
	char host[NI_MAXHOST];
	char url[URL_MAX];
	char payload[PAYLOAD_MAX];
	char *response = NULL;
	size_t response_size = 0;
	CURL *curl = NULL;
	char *encoded_cid_recov_tag = NULL;
	int err = 0;
	int res = 0;

	err = gethostname(host, sizeof(host));
	if (err) {
		LogEvent(COMPONENT_CLIENTID,
				 "Failed to gethostname: %s (%d)",
				 strerror(errno), errno);
		return;
	}

	hcloud_create_clid_name(clientid);

	if (clientid->cid_recov_tag == NULL) {
		LogEvent(COMPONENT_CLIENTID,
			 "No recovery tag for client, skipping add");
		return;
	}

	curl = curl_easy_init();
	if (curl == NULL) {
		LogEvent(COMPONENT_CLIENTID, "Failed to initialize CURL");
		return;
	}

	encoded_cid_recov_tag = curl_easy_escape(curl,
		clientid->cid_recov_tag, strlen(clientid->cid_recov_tag));
	if (encoded_cid_recov_tag == NULL) {
		LogEvent(COMPONENT_CLIENTID, "Failed to escape recovery tag");
		curl_easy_cleanup(curl);
		return;
	}

	LogEvent(COMPONENT_CLIENTID,
			 "Add client '%s' to recovery backend %s",
			 clientid->cid_recov_tag, host);

	snprintf(url, sizeof(url), "%s/%s/%s",
		recov_url, host, encoded_cid_recov_tag);

	snprintf(payload, sizeof(payload), "{\"version\": \"%s\"}", recov_version);

	curl_free(encoded_cid_recov_tag);
	encoded_cid_recov_tag = NULL;

	PTHREAD_RWLOCK_wrlock(&recov_lock);
	res = http_call(HTTP_PUT, url, payload, strlen(payload), &response, &response_size);
	PTHREAD_RWLOCK_unlock(&recov_lock);
	if (res != 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to create client in recovery backend. "
			"HTTP call error: res=%d", res);
	}
	free(response);
	curl_easy_cleanup(curl);
}

static void hcloud_rm_clid(nfs_client_id_t *clientid)
{
	char host[NI_MAXHOST];
	char url[URL_MAX];
	char *response = NULL;
	size_t response_size = 0;
	CURL *curl = NULL;
	char *encoded_cid_recov_tag = NULL;
	int err = 0;
	int res = 0;

	err = gethostname(host, sizeof(host));
	if (err) {
		LogEvent(COMPONENT_CLIENTID,
				 "Failed to gethostname: %s (%d)",
				 strerror(errno), errno);
		return;
	}

	if (clientid->cid_recov_tag == NULL) {
		LogEvent(COMPONENT_CLIENTID,
			 "No recovery tag for client, skipping remove");
		return;
	}

	curl = curl_easy_init();
	if (curl == NULL) {
		LogEvent(COMPONENT_CLIENTID, "Failed to initialize CURL");
		return;
	}

	encoded_cid_recov_tag = curl_easy_escape(curl,
		clientid->cid_recov_tag, strlen(clientid->cid_recov_tag));
	if (encoded_cid_recov_tag == NULL) {
		LogEvent(COMPONENT_CLIENTID, "Failed to escape recovery tag");
		curl_easy_cleanup(curl);
		return;
	}

	gsh_free(clientid->cid_recov_tag);
	clientid->cid_recov_tag = NULL;

	LogEvent(COMPONENT_CLIENTID,
			 "Remove client '%s' from recovery backend %s (%s)",
			 encoded_cid_recov_tag, host, encoded_cid_recov_tag);

	snprintf(url, sizeof(url), "%s/%s/%s",
		recov_url, host, encoded_cid_recov_tag);

	curl_free(encoded_cid_recov_tag);
	encoded_cid_recov_tag = NULL;

	PTHREAD_RWLOCK_wrlock(&recov_lock);
	res = http_call(HTTP_DELETE, url, NULL, 0, &response, &response_size);
	PTHREAD_RWLOCK_unlock(&recov_lock);
	if (res != 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to remove client in recovery backend. "
			"HTTP call error: res=%d (%s)", res,
			response ? response : "(none)");
	}

	free(response);
	curl_easy_cleanup(curl);
}

static int read_clids(char *response)
{
	struct json_object *obj = NULL, *clients_obj = NULL;
	size_t num_clids = 0;
	int error = -1;

	if (response == NULL) {
		LogEvent(COMPONENT_CLIENTID,
			 "Empty response from recovery backend");
		return -1;
	}

	LogDebug(COMPONENT_CLIENTID, "response=%s", response);

	obj = json_tokener_parse(response);
	if (!obj) {
		LogEvent(COMPONENT_CLIENTID, "Failed to parse \"%s\": %s", response, strerror(errno));
		goto end;
	}

	clients_obj = json_object_object_get(obj, "clients");
	if (!clients_obj) {
	    error = 0;
		LogEvent(COMPONENT_CLIENTID, "clients is empty");
		goto end;
	}

	num_clids = json_object_array_length(clients_obj);
	for (size_t i = 0; i < num_clids; i++) {
		struct json_object *obj = NULL;
		const char *clid = NULL;
		clid_entry_t *ent = NULL;

		obj = json_object_array_get_idx(clients_obj, i);
		if (!obj) {
			LogEvent(COMPONENT_CLIENTID, "Failed get client object: %s", strerror(errno));
			goto end;
		}

		clid = json_object_get_string(obj);
		ent = nfs4_add_clid_entry((char *)clid, true);
		LogEvent(COMPONENT_CLIENTID, "Added %s to clid list", ent->cl_name);
	}

	error = 0;
end:
	json_object_put(obj);
	return error;
}

static void hcloud_read_recov_clids(nfs_grace_start_t *gsp)
{
	char host[NI_MAXHOST];
	char url[URL_MAX];
	char *response = NULL;
	size_t response_size = 0;
	int err = 0;
	int res = 0;

	err = gethostname(host, sizeof(host));
	if (err) {
		LogEvent(COMPONENT_CLIENTID,
				 "Failed to gethostname: %s (%d)",
				 strerror(errno), errno);
		return;
	}

	LogEvent(COMPONENT_CLIENTID, "Read clients from recovery backend %s", host);

	snprintf(url, sizeof(url), "%s/%s", recov_url, host);

	PTHREAD_RWLOCK_rdlock(&recov_lock);
	res = http_call(HTTP_GET, url, NULL, 0, &response, &response_size);
	PTHREAD_RWLOCK_unlock(&recov_lock);
	if (res != 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to read clients from recovery backend. "
			"HTTP call error: res=%d (%s)", res,
			response ? response : "(none)");
		free(response);
		return;
	}

	read_clids(response);
	free(response);
}

static void hcloud_add_revoke_fh(nfs_client_id_t *delr_clid, nfs_fh4 *delr_handle)
{
	char host[NI_MAXHOST];
	char url[URL_MAX];
	char payload[PAYLOAD_MAX];
	char *response = NULL;
	size_t response_size = 0;
	char rhdlstr[NAME_MAX];
	CURL *curl = NULL;
	char *encoded_cid_recov_tag = NULL;
	char *encoded_rhdlstr = NULL;
	int retval = 0;
	int res = 0;
	int err = 0;

	err = gethostname(host, sizeof(host));
	if (err) {
		LogEvent(COMPONENT_CLIENTID,
				 "Failed to gethostname: %s (%d)",
				 strerror(errno), errno);
		return;
	}

	if (delr_clid->cid_recov_tag == NULL) {
		LogEvent(COMPONENT_CLIENTID,
			 "No recovery tag for client, skipping revoke fh");
		return;
	}

	/* Convert nfs_fh4_val into base64 encoded string */
	retval = base64url_encode(delr_handle->nfs_fh4_val,
							  delr_handle->nfs_fh4_len,
							  rhdlstr, sizeof(rhdlstr));
	if (retval == -1) {
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to base64-encode revoke filehandle");
		return;
	}

	curl = curl_easy_init();
	if (curl == NULL) {
		LogEvent(COMPONENT_CLIENTID, "Failed to initialize CURL");
		return;
	}

	encoded_cid_recov_tag = curl_easy_escape(curl,
		delr_clid->cid_recov_tag, strlen(delr_clid->cid_recov_tag));
	if (encoded_cid_recov_tag == NULL) {
		LogEvent(COMPONENT_CLIENTID, "Failed to escape recovery tag");
		curl_easy_cleanup(curl);
		return;
	}

	encoded_rhdlstr = curl_easy_escape(curl,
		rhdlstr, strlen(rhdlstr));
	if (encoded_rhdlstr == NULL) {
		LogEvent(COMPONENT_CLIENTID, "Failed to escape revoke filehandle");
		curl_free(encoded_cid_recov_tag);
		curl_easy_cleanup(curl);
		return;
	}

	snprintf(url, sizeof(url), "%s/%s/%s/%s",
		recov_url, host, encoded_cid_recov_tag, encoded_rhdlstr);

	snprintf(payload, sizeof(payload), "{\"version\": \"%s\"}", recov_version);

	curl_free(encoded_cid_recov_tag);
	encoded_cid_recov_tag = NULL;

	curl_free(encoded_rhdlstr);
	encoded_rhdlstr = NULL;

	PTHREAD_RWLOCK_wrlock(&recov_lock);
	res = http_call(HTTP_PUT, url, payload, strlen(payload), &response, &response_size);
	PTHREAD_RWLOCK_unlock(&recov_lock);
	if (res != 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to add revoke fh in recovery backend. "
			"HTTP call error: res=%d (%s)", res,
			response ? response : "(none)");
	}
	free(response);
	curl_easy_cleanup(curl);
}

static struct nfs4_recovery_backend hcloud_backend = {
	.recovery_init = hcloud_recov_init,
	.end_grace = hcloud_recov_end_grace,
	.recovery_read_clids = hcloud_read_recov_clids,
	.add_clid = hcloud_add_clid,
	.rm_clid = hcloud_rm_clid,
	.add_revoke_fh = hcloud_add_revoke_fh,
};

void hcloud_backend_init(struct nfs4_recovery_backend **backend)
{
	*backend = &hcloud_backend;
}
