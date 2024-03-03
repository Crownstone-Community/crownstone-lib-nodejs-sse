import {config} from "./config";

export interface SseClassInterface {
  login(email, password):                     Promise<void>,
  loginHashed(email, sha1passwordHash):       Promise<void>,
  hubLogin(hubId : string, hubToken: string): Promise<void>,
  setAccessToken(token):                      void,

  start(eventCallback : (data : SseEvent) => void) : Promise<void>,
  retryWithNewAccessToken()                        : Promise<void> | void,
  stop(): void,
  closeEventSource(): void,

  log: any
}


export const SseClassGenerator = function(options: sseConstructorOptions) : { new(options?: sseOptions): SseClassInterface; prototype: SseClassInterface } {

  const log         = options.log;
  const sha1        = options.sha1;
  const EventSource = options.EventSource;
  const fetch       = options.fetch;

  const set_Timeout    = options.setTimeout    ?? setTimeout;
  const set_Interval   = options.setInterval   ?? setInterval;
  const clear_Timeout  = options.clearTimeout  ?? clearTimeout;
  const clear_Interval = options.clearInterval ?? clearInterval;

  const defaultHeaders = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  };


  const DEFAULT_URLS = {
    sseUrl:       "https://events.ownstone.org/sse",
    loginUrl:     "https://cloud.ownstone.org/api/users/login",
    hubLoginBase: "https://cloud.ownstone.org/api/Hubs/"
  }


  return class CrownstoneSSE {
    log = log;

    listeners = {
      open:    ()      => {},
      message: (event) => {},
      error:   (error) => {}
    }


    requireAuthentication : boolean = true;
    autoreconnect         : boolean = false;

    eventSource      : any           = null;
    accessToken      : string | null = null;

    eventCallback    : (data: SseEvent) => void;

    checkerInterval  = null;
    reconnectTimeout = null;
    pingTimeout      = null;
    projectName      = null;

    sse_url          = DEFAULT_URLS.sseUrl;
    login_url        = DEFAULT_URLS.loginUrl;
    hubLogin_baseUrl = DEFAULT_URLS.hubLoginBase;

    cachedLoginData  : cachedLoginData = null;

    constructor( options? : sseOptions ) {
      this.sse_url               = options?.sseUrl       ?? DEFAULT_URLS.sseUrl;
      this.login_url             = options?.loginUrl     ?? DEFAULT_URLS.loginUrl;
      this.hubLogin_baseUrl      = options?.hubLoginBase ?? DEFAULT_URLS.hubLoginBase;
      this.projectName           = options?.projectName  ?? "no_project_name";

      this.projectName = `crownstone-lib-nodejs-sse-${config.version}-${this.projectName}`;

      if (this.hubLogin_baseUrl.substr(-1,1) !== '/') { this.hubLogin_baseUrl += "/"; }
      this.autoreconnect         = (options && options.autoreconnect !== undefined) ? options.autoreconnect : true;
      this.requireAuthentication = (options && options.requireAuthentication !== undefined) ? options.requireAuthentication : true;
    }

    async login(email, password) {
      return await this.loginHashed(email, sha1(password))
    }

    async loginHashed(email, sha1passwordHash) {
      this.cachedLoginData = {user: {email: email, hashedPassword: sha1passwordHash}};
      return fetch(
        this.login_url,
        {method:"POST", headers:defaultHeaders, body: JSON.stringify({email, password:sha1passwordHash})}
      )
        .then((result) => {
          return result.json()
        })
        .then((result) => {
          if (result?.error?.statusCode == 401) {
            throw result.error
          }
          this.accessToken = result.id;
          log.info("SSE user login successful.");
        })
        .catch((err) => {
          log.warn("SSE user login failed.", err);
          if (err?.code === "LOGIN_FAILED_EMAIL_NOT_VERIFIED") {
            console.info("This email address has not been verified yet.");
            throw err;
          }
          else if (err?.code === "LOGIN_FAILED") {
            console.info("Incorrect email/password");
            throw err;
          }
          else {
            console.error("Unknown error while trying to login to", this.login_url);
            throw err;
          }
        })
    }

    async hubLogin(hubId : string, hubToken: string) {
      this.cachedLoginData = {hub: {hubId: hubId, hubToken: hubToken}};
      let combinedUrl = this.hubLogin_baseUrl + hubId + '/login?token=' + hubToken;
      return fetch(
        combinedUrl,
        {method:"POST", headers:defaultHeaders}
      )
        .then((result) => {
          return result.json()
        })
        .then((result) => {
          if (result?.error?.statusCode == 401) {
            throw result.error
          }
          this.accessToken = result.id;
          log.info("SSE hub login successful.");
        })
        .catch((err) => {
          log.warn("SSE hub login failed.", err);
          if (err?.code === "LOGIN_FAILED") {
            console.info("Incorrect email/password");
            throw err;
          }
          else {
            console.error("Unknown error while trying to login to", combinedUrl);
            throw err;
          }
        })
    }

    async retryLogin() {
      if (this.cachedLoginData.hub !== undefined) {
        return this.hubLogin(this.cachedLoginData.hub.hubId, this.cachedLoginData.hub.hubToken)
      }
      else if (this.cachedLoginData.user !== undefined) {
        return this.loginHashed(this.cachedLoginData.user.email, this.cachedLoginData.user.hashedPassword)
      }
      throw "NO_CREDENTIALS";
    }


    setAccessToken(token) {
      this.accessToken = token;
    }


    stop() {
      this.autoreconnect = false;
      this.closeEventSource();
    }

    closeEventSource() {
      this._clearPendingActions();
      if (this.eventSource !== null) {
        this.eventSource.removeEventListener('open',    this.listeners.open);
        this.eventSource.removeEventListener('message', this.listeners.message);
        this.eventSource.removeEventListener('error',   this.listeners.error);
        this.eventSource.close();
        this.eventSource = null;
      }
    }


    /**
     * The cloud will ping every 30 seconds. If this is not received after 40 seconds, we restart the connection.
     * @private
     */
    _messageReceived() {
      clear_Timeout(this.pingTimeout);
      this.pingTimeout = set_Timeout(() => {
        if (this.eventCallback !== undefined) {
          this.start(this.eventCallback);
        }
      }, 40000);
    }


    _clearPendingActions() {
      clear_Interval(this.checkerInterval);
      clear_Timeout( this.reconnectTimeout);
      clear_Timeout( this.pingTimeout);
    }


    async start(eventCallback : (data : SseEvent) => void) : Promise<void> {
      if (this.accessToken === null && this.requireAuthentication === true) {
        throw "AccessToken is required. Use .setAccessToken() or .login() to set one."
      }

      this.eventCallback = eventCallback;

      this._clearPendingActions();
      if (this.eventSource !== null) {
        log.info("Event source closed before starting again.");
        this.closeEventSource();
      }

      return new Promise((resolve, reject) => {
        let url = this.sse_url;
        if (this.requireAuthentication === true) {
          url = this.sse_url + "?accessToken=" + this.accessToken + "&projectName=" + this.projectName;
        }

        this.eventSource = new EventSource(url);

        this.listeners.open = () => {
          log.info("Event source connection established.");

          this._messageReceived();

          this.checkerInterval = set_Interval(() => {
            if (this.eventSource.readyState === 2) { // 2 == CLOSED
              log.warn("Recovering connection....");
              this.start(this.eventCallback);
            }
          }, 1000);

          resolve();
        };


        this.listeners.message = (event) => {
          // bump the heartbeat timer.
          this._messageReceived();
          if (event?.data) {
            let message = JSON.parse(event.data);
            log.debug("Event received", message);

            this.eventCallback(message as any);
            // attempt to automatically reconnect if the token has expired.
            if (message.type === 'system' && message.code === 401 && message.subType == "TOKEN_EXPIRED") {
              this.retryWithNewAccessToken()
            }
            if (message.type === 'system' && message.code === 401 && message.subType == "INVALID_ACCESS_TOKEN") {
              this.retryWithNewAccessToken()
            }
          }
        }

        this.listeners.error = (error) => {
          clear_Interval(this.checkerInterval);
          clear_Timeout(this.reconnectTimeout);
          log.warn("Eventsource error",error);
          log.info("Reconnecting after error. Will start in 2 seconds.");
          this.closeEventSource();
          this.reconnectTimeout = set_Timeout(() => { this.start(this.eventCallback) }, 2000);
        }

        this.eventSource.addEventListener('open',    this.listeners.open);
        this.eventSource.addEventListener('message', this.listeners.message);
        this.eventSource.addEventListener('error',   this.listeners.error);

      })
    }

    retryWithNewAccessToken() : Promise<void> | void{
      this.closeEventSource();
      if (this.autoreconnect && this.cachedLoginData) {
        try {
          log.debug("Attempting to login again since our token expired...");
          return this.retryLogin()
            .then(() => {
              log.debug("Done...");
              return new Promise((resolve, reject) => { set_Timeout(resolve, 2000); });
            })
            .then(() => {
              log.debug("Retry with new token...");
              return this.start(this.eventCallback);
            })
            .then(() => {
              log.debug("Done...");
            })
        }
        catch (e) {
          let errorEvent : SseEvent = {
            type:     "system",
            subType:  "COULD_NOT_REFRESH_TOKEN",
            code:     401,
            message:  "Token expired, autoreconnect tried to get a new one. This was not successful. Connection closed.",
          };
          log.error(errorEvent)
          this.eventCallback(errorEvent);
        }
      }
      else {
        let errorEvent : SseEvent = {
          type:     "system",
          subType:  "COULD_NOT_REFRESH_TOKEN",
          code:     401,
          message:  "Token expired, autoconnect is disabled or does not have login credentials. Connection closed.",
        };
        log.error(errorEvent)
        this.eventCallback(errorEvent);
      }
    }
  }
}
