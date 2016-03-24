-module(twerl_stream).
-export([
          connect/4,
          handle_connection/3
        ]).

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").
-define(SEP, <<"\r\n">>).
-define(HTTPC_PROFILE, ?MODULE).

-spec connect(string(), list(), string(), fun()) -> ok | {error, reason}.
connect({post, Url}, Auth, Params, Callback) ->
    {Headers, Body} = case twerl_util:headers_for_auth(Auth, {post, Url}, Params) of
                        L when is_list(L) -> {L, Params};
                        {H, L2} -> {H, L2}
                      end,
    connect_1(post, {Url, Headers, ?CONTENT_TYPE, Body}, Callback);
connect({get, BaseUrl}, Auth, Params, Callback) ->
    {Headers, UrlParams} = case twerl_util:headers_for_auth(Auth, {get, BaseUrl}, Params) of
                        L when is_list(L) -> {L, Params};
                        {H, L2} -> {H, L2}
                      end,
    Url = BaseUrl ++ "?" ++ UrlParams,
    connect_1(get, {Url, Headers}, Callback).


connect_1(Method, UrlArgs, Callback) ->
    lager:debug("twerl_stream - connecting to url with ~p: ~p", [Method, UrlArgs]),
    start_httpc_profile(),
    case catch httpc:request(
                        Method, 
                        UrlArgs,
                        [],
                        [{sync, false}, {stream, self}],
                        ?HTTPC_PROFILE)
    of
        {ok, RequestId} ->
            FinalResult = handle_connection(Callback, RequestId, <<>>),
            _ = httpc:cancel_request(RequestId, ?HTTPC_PROFILE),
            FinalResult;
        {error, Reason} ->
            {error, {http_error, Reason}}
    end.

start_httpc_profile() ->
    case inets:start(httpc, [{profile, ?HTTPC_PROFILE}]) of
        {ok, _Pid} ->
            httpc:set_options([
                    {pipeline_timeout, 90000},
                    {max_sessions, 100},
                    {cookies, disabled}
                ], ?HTTPC_PROFILE),
            ok;
        {error, {already_started, _Pid}} ->
            cleanup_sessions(),
            ok
    end.

% httpc can leave sessions with dead handler processes in the session table.
cleanup_sessions() ->
    {Sessions, _, _} = httpc:which_sessions(twerl_stream),
    % A session looks like: 
    % {session,{{"stream.twitter.com",443},<0.6468.4>},false,https,undefined,undefined,1,pipeline,false}
    lists:foreach(
        fun(Session) ->
            Id = element(2, Session),
            HandlerPid = element(2, Id),
            case is_process_alive(HandlerPid) of
                true -> ok;
                false -> httpc_manager:delete_session(Id, httpc:profile_name(twerl_stream))
            end
        end,
        Sessions).


-spec handle_connection(term(), term(), binary()) -> {ok, terminate} | {ok, stream_end} | {error, term()}.
handle_connection(Callback, RequestId, Buffer) ->
    receive
        % stream opened
        {http, {RequestId, stream_start, _Headers}} ->
            ?MODULE:handle_connection(Callback, RequestId, Buffer);

        % stream received data
        {http, {RequestId, stream, Data}} ->
            NewBuffer = handle_data(Callback, Data, Buffer),
            ?MODULE:handle_connection(Callback, RequestId, NewBuffer);

        % stream closed
        {http, {RequestId, stream_end, _Headers}} ->
            {ok, stream_end};

        % connected but received error cod
        % 401 unauthorized - authentication credentials rejected
        {http, {RequestId, {{_, 401, _}, _Headers, Body}}} ->
            {error, {unauthorized, Body}};

        % 404 not found
        {http, {RequestId, {{_, 404, _}, _Headers, Body}}} ->
            {error, {notfound, Body}};

        % 406 not acceptable - invalid request to the search api
        {http, {RequestId, {{_, 406, _}, _Headers, Body}}} ->
            {error, {invalid_params, Body}};

        % 420 enhance your calm - Exceeded connection limit for user
        {http, {RequestId, {{_, 420, _}, _Headers, Body}}} ->
            {error, {connection_limit, Body}};

        % Other HTTP status codes
        {http, {RequestId, {{_, Code, _}, _Headers, Body}}} ->
            {error, {Code, Body}};

        % connection error
        % may happen while connecting or after connected
        {http, {RequestId, {error, Reason}}} ->
            {error, {http_error, Reason}};

        % message send by us to close the connection
        terminate ->
            lager:debug("Twitter stream: terminate requested"),
            {ok, terminate};

        Other ->
            lager:error("Twitter stream: unknown message ~p", [Other]),
            {error, unknown_message}
            % twerl_stream:handle_connection(Callback, RequestId, Buffer)

    after 90*1000 ->
        {ok, stream_end}
    end.


handle_data(_Callback, ?SEP, <<>>) ->
    <<>>;
handle_data(Callback, Data, Chunk) ->
    Chunk1 = append(Data, Chunk),
    Lines = binary:split(Chunk1, ?SEP, [global]),
    send_json(Callback, Lines, Data =:= ?SEP).

send_json(_Callback, [], _IsComplete) ->
    <<>>;
send_json(_Callback, [Chunk], false) ->
    Chunk;
send_json(Callback, [<<>>|Lines], IsComplete) ->
    send_json(Callback, Lines, IsComplete);
send_json(Callback, [Line|Lines], IsComplete) ->
    spawn(fun() -> 
              Callback(twerl_util:decode(Line)) 
          end),
    send_json(Callback, Lines, IsComplete).

append(<<>>, B) -> B;
append(?SEP, B) -> B;
append(A, <<>>) -> A;
append(A, B) -> <<B/binary, A/binary>>.

