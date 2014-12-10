-module(twerl_stream).
-export([
          connect/4,
          handle_connection/3
        ]).

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").
-define(SEP, <<"\r\n">>).

-spec connect(string(), list(), string(), fun()) -> ok | {error, reason}.
connect({post, Url}, Auth, Params, Callback) ->
    {Headers, Body} = case twerl_util:headers_for_auth(Auth, {post, Url}, Params) of
                        L when is_list(L) ->
                            {L, Params};
                          {H, L2} ->
                              {H, L2}
                      end,
    lager:debug("twerl_stream - connecting to url with POST: ~p", [Url]),
    httpc:set_options([{pipeline_timeout, 90000}]),
    case catch httpc:request(post, {Url, Headers, ?CONTENT_TYPE, Body}, [], [{sync, false}, {stream, self}]) of
        {ok, RequestId} ->
            handle_connection(Callback, RequestId, <<>>);
        {error, Reason} ->
            {error, {http_error, Reason}}
    end;

connect({get, BaseUrl}, Auth, Params, Callback) ->
    {Headers, UrlParams} = case twerl_util:headers_for_auth(Auth, {get, BaseUrl}, Params) of
                        L when is_list(L) ->
                            {L, Params};
                          {H, L2} ->
                              {H, L2}
                      end,
    Url = BaseUrl ++ "?" ++ UrlParams,
    lager:debug("twerl_stream - connecting to url with GET: ~p", [Url]),
    httpc:set_options([{pipeline_timeout, 90000}]),
    case catch httpc:request(get, {Url, Headers}, [], [{sync, false}, {stream, self}]) of
        {ok, RequestId} ->
            handle_connection(Callback, RequestId, <<>>);
        {error, Reason} ->
            {error, {http_error, Reason}}
    end.

% TODO maybe change {ok, stream_closed} to an error?
-spec handle_connection(term(), term(), binary()) -> {ok, terminate} | {ok, stream_end} | {error, term()}.
handle_connection(Callback, RequestId, Buffer) ->
    receive
        % stream opened
        {http, {RequestId, stream_start, _Headers}} ->
            twerl_stream:handle_connection(Callback, RequestId, Buffer);

        % stream received data
        {http, {RequestId, stream, Data}} ->
            NewBuffer = handle_data(Callback, Data, Buffer),
            twerl_stream:handle_connection(Callback, RequestId, NewBuffer);

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
            httpc:cancel_request(RequestId),
            {ok, terminate};

        Other ->
            lager:warning("Twitter stream: unknown message ~p", [Other]),
            twerl_stream:handle_connection(Callback, RequestId, Buffer)

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

