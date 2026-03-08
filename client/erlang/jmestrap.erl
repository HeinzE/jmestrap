%%% @doc JMESTrap client — Erlang API for the JMESTrap REST server.
%%%
%%% Functional client using tagged tuples and OTP's built-in `httpc'
%%% and `json' (OTP 27+). No external dependencies.
%%%
%%% == Quick start ==
%%%
%%% ```
%%% 1> jmestrap:start().
%%% ok
%%% 2> T = jmestrap:new("http://127.0.0.1:9000").
%%% 3> jmestrap:ping(T).
%%% ok
%%% 4> {ok, Ref} = jmestrap:record(T, #{
%%%        sources => [<<"sensor_1">>],
%%%        until => {order, [<<"event == 'start'">>, <<"event == 'done'">>]}
%%%    }).
%%% 5> {ok, Result} = jmestrap:fetch(T, Ref, 15).
%%% 6> maps:get(<<"finished">>, Result).
%%% true
%%% '''
%%%
%%% == CommonTest integration ==
%%%
%%% ```
%%% init_per_suite(Config) ->
%%%     jmestrap:start(),
%%%     Trap = jmestrap:new("http://127.0.0.1:9000"),
%%%     [{trap, Trap} | Config].
%%%
%%% end_per_suite(Config) ->
%%%     jmestrap:cleanup(?config(trap, Config)),
%%%     Config.
%%%
%%% test_login_logout(Config) ->
%%%     T = ?config(trap, Config),
%%%     {ok, Ref} = jmestrap:record(T, #{
%%%         sources => [<<"keyboard_1">>],
%%%         until => {order, [<<"event == 'login'">>, <<"event == 'logout'">>]}
%%%     }),
%%%     keyboard:login(1743),
%%%     {ok, #{<<"finished">> := true}} = jmestrap:fetch(T, Ref, 15).
%%% '''
-module(jmestrap).

-export([
    start/0,
    new/1,
    ping/1,
    record/2,
    fetch/3,
    stop/2,
    delete/2,
    inject/3,
    list_recordings/1,
    list_sources/1,
    cleanup/1
]).

-export_type([trap/0, ref/0, until/0]).

%% Opaque client handle — just the base URL.
-opaque trap() :: #{base := string()}.

%% Server-assigned recording reference.
-type ref() :: non_neg_integer().

%% Completion condition.
-type until() :: {order, [binary()]} | {any_order, [binary()]}.

%% Recording options.
-type record_opts() :: #{
    description => binary(),
    sources => [binary()],
    matching => binary(),
    until => until()
}.

%%% ===================================================================
%%% Setup
%%% ===================================================================

%% @doc Start required OTP applications (inets, ssl). Idempotent.
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(ssl),
    ok.

%%% ===================================================================
%%% Client
%%% ===================================================================

%% @doc Create a client handle pointing at the given base URL.
-spec new(string()) -> trap().
new(BaseUrl) ->
    Base = string:trim(BaseUrl, trailing, "/"),
    #{base => Base}.

%% @doc Health check. Returns `ok' or `{error, Reason}'.
-spec ping(trap()) -> ok | {error, term()}.
ping(#{base := Base}) ->
    case get_json(Base ++ "/ping") of
        {ok, 200, _} -> ok;
        {ok, Status, Body} -> {error, {http, Status, Body}};
        {error, Reason} -> {error, Reason}
    end.

%% @doc Start a recording. Returns `{ok, Ref}' or `{error, Reason}'.
-spec record(trap(), record_opts()) -> {ok, ref()} | {error, term()}.
record(#{base := Base}, Opts) ->
    Body = encode_record_opts(Opts),
    case post_json(Base ++ "/recordings", Body) of
        {ok, 201, #{<<"reference">> := Ref}} ->
            {ok, Ref};
        {ok, 422, #{<<"error">> := Msg}} ->
            {error, {invalid_predicate, Msg}};
        {ok, Status, Json} ->
            {error, {http, Status, Json}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Long-poll for recording results.
%%
%% Waits up to `Timeout' seconds for completion.
%% Returns `{ok, Result}' where Result is the full server response
%% (map with keys: reference, sources, finished, active, event_count,
%% events, matching_expr, until).
-spec fetch(trap(), ref(), number()) ->
    {ok, map()} | {error, term()}.
fetch(#{base := Base}, Ref, Timeout) ->
    Url = Base ++ "/recordings/" ++ integer_to_list(Ref)
        ++ "?timeout=" ++ encode_number(Timeout),
    case get_json(Url) of
        {ok, 200, Json} ->
            {ok, Json};
        {ok, 404, _} ->
            {error, {not_found, Ref}};
        {ok, Status, Body} ->
            {error, {http, Status, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop a recording early.
-spec stop(trap(), ref()) -> ok | {error, term()}.
stop(#{base := Base}, Ref) ->
    Url = Base ++ "/recordings/" ++ integer_to_list(Ref) ++ "/stop",
    case post_json(Url, #{}) of
        {ok, Status, _} when Status >= 200, Status < 300 -> ok;
        {ok, Status, Body} -> {error, {http, Status, Body}};
        {error, Reason} -> {error, Reason}
    end.

%% @doc Delete a recording.
-spec delete(trap(), ref()) -> ok | {error, term()}.
delete(#{base := Base}, Ref) ->
    Url = Base ++ "/recordings/" ++ integer_to_list(Ref),
    case request(delete, Url) of
        {ok, Status, _} when Status >= 200, Status < 300 -> ok;
        {ok, Status, Body} -> {error, {http, Status, Body}};
        {error, Reason} -> {error, Reason}
    end.

%% @doc Inject an event directly (for testing / synthetic events).
-spec inject(trap(), binary(), map()) -> ok | {error, term()}.
inject(#{base := Base}, Source, Payload) ->
    Url = Base ++ "/events/" ++ binary_to_list(Source),
    case post_json(Url, Payload) of
        {ok, Status, _} when Status >= 200, Status < 300 -> ok;
        {ok, Status, Body} -> {error, {http, Status, Body}};
        {error, Reason} -> {error, Reason}
    end.

%% @doc List all recordings.
-spec list_recordings(trap()) -> {ok, [map()]} | {error, term()}.
list_recordings(#{base := Base}) ->
    case get_json(Base ++ "/recordings") of
        {ok, 200, #{<<"recordings">> := Recs}} -> {ok, Recs};
        {ok, Status, Body} -> {error, {http, Status, Body}};
        {error, Reason} -> {error, Reason}
    end.

%% @doc List observed event sources.
-spec list_sources(trap()) -> {ok, [map()]} | {error, term()}.
list_sources(#{base := Base}) ->
    case get_json(Base ++ "/sources") of
        {ok, 200, #{<<"sources">> := Sources}} -> {ok, Sources};
        {ok, Status, Body} -> {error, {http, Status, Body}};
        {error, Reason} -> {error, Reason}
    end.

%% @doc Delete all recordings. Useful for test teardown.
-spec cleanup(trap()) -> ok.
cleanup(Trap) ->
    case list_recordings(Trap) of
        {ok, Recs} ->
            lists:foreach(
                fun(#{<<"reference">> := Ref}) -> delete(Trap, Ref) end,
                Recs
            );
        {error, _} ->
            ok
    end.

%%% ===================================================================
%%% Internal — Record option encoding
%%% ===================================================================

-spec encode_record_opts(record_opts()) -> map().
encode_record_opts(Opts) ->
    M0 = #{},
    M1 = case maps:find(description, Opts) of
        {ok, Desc} when Desc =/= <<>> -> M0#{<<"description">> => Desc};
        _ -> M0
    end,
    M2 = case maps:find(sources, Opts) of
        {ok, S} when S =/= [] -> M1#{<<"sources">> => S};
        _ -> M1
    end,
    M3 = case maps:find(matching, Opts) of
        {ok, Expr} -> M2#{<<"matching">> => Expr};
        error -> M2
    end,
    case maps:find(until, Opts) of
        {ok, Until} -> M3#{<<"until">> => encode_until(Until)};
        error -> M3
    end.

-spec encode_until(until()) -> map().
encode_until({order, Predicates}) ->
    #{<<"type">> => <<"order">>, <<"predicates">> => Predicates};
encode_until({any_order, Predicates}) ->
    #{<<"type">> => <<"any_order">>, <<"predicates">> => Predicates}.

-spec encode_number(number()) -> string().
encode_number(N) when is_integer(N) ->
    integer_to_list(N);
encode_number(N) when is_float(N) ->
    float_to_list(N, [{decimals, 1}, compact]).

%%% ===================================================================
%%% Internal — HTTP via httpc + json (OTP 27+)
%%% ===================================================================

-spec get_json(string()) -> {ok, integer(), term()} | {error, term()}.
get_json(Url) ->
    request(get, Url).

-spec post_json(string(), map()) -> {ok, integer(), term()} | {error, term()}.
post_json(Url, Body) ->
    JsonBin = iolist_to_binary(json:encode(Body)),
    case httpc:request(
        post,
        {Url, [], "application/json", JsonBin},
        [{timeout, 120000}],
        [{body_format, binary}]
    ) of
        {ok, {{_, Status, _}, _Headers, RespBody}} ->
            {ok, Status, decode_body(RespBody)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec request(get | delete, string()) ->
    {ok, integer(), term()} | {error, term()}.
request(Method, Url) ->
    case httpc:request(
        Method,
        {Url, []},
        [{timeout, 120000}],
        [{body_format, binary}]
    ) of
        {ok, {{_, Status, _}, _Headers, RespBody}} ->
            {ok, Status, decode_body(RespBody)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec decode_body(binary()) -> term().
decode_body(<<>>) -> #{};
decode_body(Bin) -> json:decode(Bin).
