
/*
 * Odyssey.
 *
 * Scalable PostgreSQL connection pooler.
 */

#include <kiwi.h>
#include <machinarium.h>
#include <odyssey.h>

int od_deploy(od_client_t *client, char *context)
{
	od_instance_t *instance = client->global->instance;
	od_server_t *server = client->server;
	od_route_t *route = client->route;

	if (route->id.physical_rep || route->id.logical_rep) {
		return 0;
	}

	/* compare and set options which are differs from server */
	int query_count;
	query_count = 0;

	char query[OD_QRY_MAX_SZ];
	int query_size;
	query_size = kiwi_vars_cas(&client->vars, &server->vars, query,
				   sizeof(query) - 1);

	if (query_size > 0) {
		query[query_size] = 0;
		query_size++;
		machine_msg_t *msg;
		msg = kiwi_fe_write_query(NULL, query, query_size);
		if (msg == NULL)
			return -1;

		int rc;
		rc = od_write(&server->io, msg);
		if (rc == -1)
			return -1;

		query_count++;
		client->server->synced_settings = false;

		od_debug(&instance->logger, context, client, server,
			 "deploy: %s", query);
	} else {
		client->server->synced_settings = true;
	}

	/* Changes for the SET ROLE */
	{
	char set_role_query[OD_QRY_MAX_SZ];
	sprintf(set_role_query,"SET role = %s ;",client->startup.user.value);
	int set_role_len = strlen(set_role_query);
	set_role_query[set_role_len] = 0 ;
	set_role_len++;

	machine_msg_t *msg;
	msg = kiwi_fe_write_query(NULL, set_role_query, set_role_len);
	if (msg == NULL)
		return -1;

	int rc;
	rc = od_write(&server->io, msg);
	if (rc == -1)
		return -1;
	
	client->server->synced_settings = false;

	od_debug(&instance->logger, context, client, server,
			 "deploy: %s", set_role_query);
	query_count++;
	}



	return query_count;
}
