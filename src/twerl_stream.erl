-module(twerl_stream).
-export([
          connect/4
        ]).

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

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
            handle_connection(Callback, RequestId, []);
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
            handle_connection(Callback, RequestId, []);
        {error, Reason} ->
            {error, {http_error, Reason}}
    end.

% TODO maybe change {ok, stream_closed} to an error?
-spec handle_connection(term(), term(), [binary()]) -> {ok, terminate} | {ok, stream_end} | {error, term()}.
handle_connection(Callback, RequestId, Buffer) ->
    receive
        % stream opened
        {http, {RequestId, stream_start, _Headers}} ->
            handle_connection(Callback, RequestId, Buffer);

        % stream received data
        {http, {RequestId, stream, Data}} ->
            NewBuffer = handle_data(Callback, Data, Buffer),
            handle_connection(Callback, RequestId, NewBuffer);

        % stream closed
        {http, {RequestId, stream_end, _Headers}} ->
            {ok, stream_end};

        % connected but received error cod
        % 401 unauthorised - authentication credentials rejected
        {http, {RequestId, {{_, 401, _}, _Headers, _Body}}} ->
            {error, unauthorised};

        % 406 not acceptable - invalid request to the search api
        {http, {RequestId, {{_, 406, _}, _Headers, Body}}} ->
            {error, {invalid_params, Body}};

        % connection error
        % may happen while connecting or after connected
        {http, {RequestId, {error, Reason}}} ->
            {error, {http_error, Reason}};

        % message send by us to close the connection
        terminate ->
            {ok, terminate}
    after 90*1000 ->
            {ok, stream_end}
    end.


handle_data(Callback, Line, Buffer) ->
    case binary:split(Line, <<"\r\n">>) of
        [Part] ->
            [Part|Buffer];
        [End, Rest] ->
            spawn(fun() ->
                          JsonBin = iolist_to_binary(lists:reverse([End|Buffer])),
                          DecodedData = twerl_util:decode(JsonBin),
                          Callback(DecodedData)
                  end),
            case Rest of
                <<>> -> [];
                _ -> [Rest]
            end
    end.

