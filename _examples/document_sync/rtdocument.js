class RealTimeDocument {
    #subscription;
    #channel;
    #load;
    #applyUpdate;
    #compareVersion;
    #onChange;
    #messageBuffer = [];
    #document = null;
    #version = null;
    #isLoaded = false;
    #reSyncTimer = null;
    #minReSyncDelay;
    #maxReSyncDelay;
    #reSyncAttempts = 0;
    #debug = false;

    constructor({
        subscription,
        load,
        applyUpdate,
        compareVersion,
        onChange,
        minReSyncDelay = 100,
        maxReSyncDelay = 10000,
        debug = false,
    }) {
        if (!subscription) throw new Error("subscription is required");
        if (!load) throw new Error("load function is required");
        if (!applyUpdate) throw new Error("applyUpdate function is required");
        if (!compareVersion) throw new Error("compareVersion function is required");
        if (!onChange) throw new Error("onChange function is required");

        this.#subscription = subscription;
        this.#channel = subscription.channel;
        this.#load = load;
        this.#applyUpdate = applyUpdate;
        this.#compareVersion = compareVersion;
        this.#onChange = onChange;
        this.#minReSyncDelay = minReSyncDelay;
        this.#maxReSyncDelay = maxReSyncDelay;
        this.#debug = debug;

        this.#subscription.on('publication', (ctx) => {
            if (!this.#isLoaded) {
                // Buffer messages until initial state is loaded.
                this.#messageBuffer.push(ctx);
                return;
            }
            // Process new messages immediately if initial state is already loaded.
            const newVersion = this.#compareVersion(ctx, this.#version);
            if (newVersion === null) {
                this.#debugLog("Skip real-time publication", ctx);
                return;
            }
            this.#document = this.#applyUpdate(this.#document, ctx.data);
            this.#version = newVersion;
            this.#onChange(this.#document);
        }).on('subscribed', (ctx) => {
            if (ctx.wasRecovering) {
                if (ctx.recovered) {
                    this.#debugLog("Successfully re-attached to a stream");
                } else {
                    this.#debugLog("Re-syncing due to failed recovery");
                    this.#reSync();
                }
            } else {
                this.#debugLog("Load data for the first time");
                this.#loadDocumentApplyBuffered().catch(error => this.#debugLog("Unhandled error in loadDocumentApplyBuffered", error));
            }
        }).on('unsubscribed', (ctx) => {
            this.#debugLog("Subscription unsubscribed", ctx);
            this.stopSync();
        });
    }

    startSync() {
        if (!this.#subscription) {
            this.#debugLog("Document already disposed", this.#channel);
            return;
        }
        this.#subscription.subscribe();
    }

    stopSync() {
        this.#stopReSync()
        this.#subscription.unsubscribe();
        this.#debugLog("Stopped and unsubscribed from channel", this.#channel);
    }

    #debugLog(...args) {
        if (!this.#debug) {
            return;
        }
        console.log(...args);
    }

    #randomInt(min, max) { // min and max included
        return Math.floor(Math.random() * (max - min + 1) + min);
    }

    #backoff(step, min, max) {
        // Full jitter technique, see:
        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        if (step > 31) { step = 31; }
        const interval = this.#randomInt(0, Math.min(max, min * Math.pow(2, step)));
        return Math.min(max, min + interval);
    }

    #stopReSync() {
        if (this.#reSyncTimer) {
            clearTimeout(this.#reSyncTimer);
            this.#reSyncTimer = null;
        }
    }

    async #loadDocumentApplyBuffered() {
        try {
            const result = await this.#load();
            this.#document = result.document;
            this.#version = result.version;
            this.#isLoaded = true;
            this.#debugLog("Initial state loaded", this.#document, "version:", this.#version);
            this.#processBufferedMessages();
            this.#reSyncAttempts = 0;
        } catch (error) {
            const delay = this.#backoff(this.#reSyncAttempts, this.#minReSyncDelay, this.#maxReSyncDelay);
            this.#reSyncAttempts++;
            this.#debugLog('Failed to load initial data', error, 'retry in', delay);
            this.#reSyncTimer = setTimeout( () => {
                this.#debugLog("Re-syncing due to failed load");
                this.#reSync();
            }, delay);
        }
    }

    #reSync() {
        this.#isLoaded = false; // Reset the flag to collect new messages to the buffer.
        this.#messageBuffer = [];
        this.#loadDocumentApplyBuffered().catch(
            error => this.#debugLog("Unhandled error in loadDocumentApplyBuffered", error));
    }

    #processBufferedMessages() {
        this.#messageBuffer.forEach((msg) => {
            const newVersion = this.#compareVersion(msg, this.#version);
            if (newVersion) {
                this.#document = this.#applyUpdate(this.#document, msg.data);
                this.#version = newVersion;
            } else {
                this.#debugLog("Skip buffered publication", msg);
            }
        });
        // Clear the buffer after processing.
        this.#messageBuffer = [];
        this.#onChange(this.#document);
    }
}
