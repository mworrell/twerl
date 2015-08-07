-module(twerl_stream_manager).
-behaviour(gen_server).

%% gen_server callbacks
-export([init/1,
        handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

%% api callbacks
-export([
          start_link/0,
          start_link/1,
          stop/1,
          start_stream/1,
          stop_stream/1,
          set_endpoint/2,
          set_params/2,
          set_callback/2,
          set_auth/2,
          status/1
        ]).

-record(state, {
        status = disconnected :: atom(),
        auth = {basic, ["", ""]} :: list(),
        params = "" :: string(),
        endpoint :: tuple(),
        callback :: term(),
        client_pid :: pid()
    }).

%%====================================================================
%% api callbacks
%%====================================================================
start_link() ->
    start_link(?MODULE).

start_link(GenServerName) ->
    Args = [],
    gen_server:start_link({local, GenServerName}, ?MODULE, Args, []).

stop(ServerRef) ->
    gen_server:call(ServerRef, stop).

start_stream(ServerRef) ->
    gen_server:call(ServerRef, start_stream).

stop_stream(ServerRef) ->
    gen_server:call(ServerRef, stop_stream).

set_params(ServerRef, Params) ->
    gen_server:call(ServerRef, {set_params, Params}).

set_endpoint(ServerRef, Endpoint) ->
    gen_server:call(ServerRef, {set_endpoint, Endpoint}).

set_callback(ServerRef, Callback) ->
    gen_server:call(ServerRef, {set_callback, Callback}).

set_auth(ServerRef, Auth) ->
    gen_server:call(ServerRef, {set_auth, Auth}).

status(ServerRef) ->
    gen_server:call(ServerRef, status).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    Endpoint = {post, twerl_util:filter_url()},
    {ok, #state{endpoint=Endpoint}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
    {stop, normal, stopped, client_shutdown(State)};

handle_call(start_stream, _From, State=#state{client_pid=undefined}) ->
    {reply, ok, client_connect(State)};
handle_call(start_stream, _From, State=#state{}) ->
    %% ignore
    {reply, ok, State};

handle_call(stop_stream, _From, State=#state{client_pid=undefined}) ->
    %% ignore
    {reply, ok, State};
handle_call(stop_stream, _From, State) ->
    {reply, ok, client_shutdown(State)};

handle_call({set_endpoint, E}, _From, State=#state{params=E}) ->
    %% same, don't do anything
    {reply, ok, State};
handle_call({set_endpoint, {_,_}=E}, _From, State=#state{client_pid=undefined}) ->
    %% set endpoint without client connection
    {reply, ok, State#state{endpoint=E}};
handle_call({set_endpoint, {_,_}=E}, _From, State) ->
    %% change and restart the client
    State1 = client_shutdown(State),
    State2 = client_connect(State1#state{endpoint=E}),
    {reply, ok, State2};

handle_call({set_params, OldParams}, _From, State=#state{params=OldParams}) ->
    %% same, don't do anything
    {reply, ok, State};
handle_call({set_params, Params}, _From, State=#state{client_pid=undefined}) ->
    %% set params without client connection
    {reply, ok, State#state{ params = Params }};
handle_call({set_params, Params}, _From, State) ->
    %% change and see if we need to restart the client
    State1 = client_shutdown(State),
    State2 = client_connect(State1#state{params=Params}),
    {reply, ok, State2};

handle_call({set_auth, OldAuth}, _From, State=#state{auth=OldAuth}) ->
    %% same, don't do anything
    {reply, ok, State};
handle_call({set_auth, Auth}, _From, State=#state{client_pid=undefined}) ->
    %% set auth without client connection
    {reply, ok, State#state{ auth = Auth }};
handle_call({set_auth, Auth}, _From, State) ->
    %% different, change and see if we need to restart the client
    State1 = client_shutdown(State),
    State2 = client_connect(State1#state{auth=Auth}),
    {reply, ok, State2};

handle_call({set_callback, Callback}, _From, State) ->
    {reply, ok, State#state{ callback = Callback }};

handle_call(status, _From, State = #state{status = Status}) ->
    {reply, Status, State};

handle_call(_Request, _From, State) ->
    lager:warning("Unknown call: ~p", [_Request]),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({client_data, Data}, State = #state{ callback = Callback }) ->
    case Callback of
        undefined ->
            % no callback set, ignore data
            ok;
        _ ->
            % callback set, call with data
            spawn(fun() -> Callback(Data) end)
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({client_exit, Reason, Pid}, State) when Pid == State#state.client_pid ->
    State1 = State#state{client_pid=undefined, status=disconnected},
    NewState
        = case Reason of
              %% Handle messages from client process terminating
              {ok, terminate} ->
                  %% We closed the connection
                  State1;
              {ok, stream_end} ->
                  client_reconnect({ok, stream_end}, State1);
              {error, {unauthorized, _}} ->
                  lager:warning("Twitter: stream is unauthorized, will not reconnect"),
                  State1#state{status=unauthorized};
              {error, {invalid_params, _}} ->
                  lager:warning("Twitter: invalid parameters, will not reconnect"),
                  State1#state{status=invalid_params};
              {error, R} ->
                  client_reconnect({error, R}, State1)
          end,
    {noreply, NewState};

handle_info(reconnect, State) ->
    %% different, change and see if we need to restart the client
    State1 = client_connect(State),
    {noreply, State1};
    
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
-spec client_connect(#state{}) -> pid().
client_connect(#state{status = connected} = State) ->
    State;
client_connect(#state{status = terminating} = State) ->
    lager:debug("Twitter: waiting for current streaming process to terminate."),
    timer:send_after(1000, reconnect),
    State;
client_connect(State=#state{auth = Auth, params = Params, endpoint = Endpoint}) ->
    Parent = self(),

    % We don't use the callback from the state, as we want to be able to change
    % it without restarting the client. As such we call back into the manager
    % which deals with the data as it sees fit
    Callback = fun(Data) ->
        gen_server:cast(Parent, {client_data, Data})
    end,

    Pid = proc_lib:spawn_link(
            fun() ->
                    proc_lib:init_ack(Parent, {ok, self()}),
                    R = twerl_stream:connect(Endpoint, Auth, Params, Callback),
                    error_logger:error_msg("Twitter stream disconnect: ~p", [R]),
                    Parent ! {client_exit, R, self()}
            end),
    State#state{client_pid=Pid, status=connected}.



-spec client_shutdown(#state{}) -> #state{}.
client_shutdown(State=#state{client_pid=undefined}) ->
    %% not started, nothing to do
    State;
client_shutdown(State=#state{client_pid=Pid}) ->
    %% terminate the client
    case is_pid(Pid) andalso is_process_alive(Pid) of
        true ->
            Pid ! terminate,
            State#state{status=terminating};
        false ->
            State#state{client_pid=undefined, status=disconnected}
    end.

client_reconnect({ok, Status}, State) ->
    client_reconnect(5, Status, State);
client_reconnect({error, {connection_limit, _} = Err}, State) ->
    client_reconnect(5*60, Err, State);
client_reconnect({error, {notfound, _} = Err}, State) ->
    client_reconnect(15*60, Err, State);
client_reconnect({error, Status}, State) ->
    client_reconnect(5, Status, State).

client_reconnect(After, Status, State) ->
    lager:warning("Twitter: will reconnect stream in ~p secs, status = ~p", [After, Status]),
    erlang:send_after(After * 1000, self(), reconnect),
    State#state{client_pid=undefined}.

    
