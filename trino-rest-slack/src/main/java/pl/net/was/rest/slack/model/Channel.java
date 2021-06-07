/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.net.was.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Channel
{
    private final String id;
    private final String name;
    private final boolean isMember;
    private final boolean isArchived;

    /*
     * {"ok":true,"channels":[
     * {"id":"C0242PH6V0E","name":"fuck-around-and-find-out","is_channel":true,"is_group":false,"is_im":false,"created":1623095691,"is_archived":false,"is_general":false,"unlinked":0,"name_normalized":"fuck-around-and-find-out","is_shared":false,"parent_conversation":null,"creator":"U0242PCE55L","is_ext_shared":false,"is_org_shared":false,"shared_team_ids":["T0249GTFJ4S"],"pending_shared":[],"pending_connected_team_ids":[],"is_pending_ext_shared":false,"is_member":false,"is_private":false,"is_mpim":false,"topic":{"value":"","creator":"","last_set":0},"purpose":{"value":"This *channel* is for working on a project. Hold meetings, share docs, and make decisions together with your team.","creator":"U0242PCE55L","last_set":1623095691},"previous_names":[],"num_members":1},
     * {"id":"C0249PYPT6E","name":"trino_rest","is_channel":true,"is_group":false,"is_im":false,"created":1623099447,"is_archived":false,"is_general":false,"unlinked":0,"name_normalized":"trino_rest","is_shared":false,"parent_conversation":null,"creator":"U0242PCE55L","is_ext_shared":false,"is_org_shared":false,"shared_team_ids":["T0249GTFJ4S"],"pending_shared":[],"pending_connected_team_ids":[],"is_pending_ext_shared":false,"is_member":false,"is_private":false,"is_mpim":false,"topic":{"value":"","creator":"","last_set":0},"purpose":{"value":"","creator":"","last_set":0},"previous_names":["trino-rest"],"num_members":1},
     * {"id":"C024FMR0A6Q","name":"random","is_channel":true,"is_group":false,"is_im":false,"created":1623095617,"is_archived":false,"is_general":false,"unlinked":0,"name_normalized":"random","is_shared":false,"parent_conversation":null,"creator":"U0242PCE55L","is_ext_shared":false,"is_org_shared":false,"shared_team_ids":["T0249GTFJ4S"],"pending_shared":[],"pending_connected_team_ids":[],"is_pending_ext_shared":false,"is_member":false,"is_private":false,"is_mpim":false,"topic":{"value":"","creator":"","last_set":0},"purpose":{"value":"This channel is for... well, everything else. It\u2019s a place for team jokes, spur-of-the-moment ideas, and funny GIFs. Go wild!","creator":"U0242PCE55L","last_set":1623095617},"previous_names":[],"num_members":1},
     * {"id":"C024ZB0UMBJ","name":"general","is_channel":true,"is_group":false,"is_im":false,"created":1623095617,"is_archived":false,"is_general":true,"unlinked":0,"name_normalized":"general","is_shared":false,"parent_conversation":null,"creator":"U0242PCE55L","is_ext_shared":false,"is_org_shared":false,"shared_team_ids":["T0249GTFJ4S"],"pending_shared":[],"pending_connected_team_ids":[],"is_pending_ext_shared":false,"is_member":false,"is_private":false,"is_mpim":false,"topic":{"value":"","creator":"","last_set":0},"purpose":{"value":"This is the one channel that will always include everyone. It\u2019s a great spot for announcements and team-wide conversations.","creator":"U0242PCE55L","last_set":1623095617},"previous_names":[],"num_members":1}
     * ],"response_metadata":{"next_cursor":""}}
     * 2021-06-07T20:58:48.943Z	INFO	main	okhttp3.OkHttpClient
     */
    @JsonCreator
    public Channel(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("is_member") boolean isMember,
            @JsonProperty("is_archived") boolean isArchived)
    {
        this.id = id;
        this.name = name;
        this.isMember = isMember;
        this.isArchived = isArchived;
    }

    public String getName()
    {
        return name;
    }

    public boolean isMember()
    {
        return isMember;
    }

    public boolean isArchived()
    {
        return isArchived;
    }

    public String getId()
    {
        return id;
    }
}
