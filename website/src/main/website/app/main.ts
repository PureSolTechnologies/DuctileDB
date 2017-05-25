import {provide} from '@angular/core';
import {bootstrap}    from '@angular/platform-browser'

import {AppComponent} from './app.component'
import {SiteConstants} from './site-constants'

import {
    ROUTER_PROVIDERS,
    PathLocationStrategy,
    LocationStrategy
} from '@angular/router';

bootstrap(AppComponent, [
    ROUTER_PROVIDERS,
    provide(LocationStrategy, {useClass: PathLocationStrategy}),
    provide(SiteConstants, {useClass: SiteConstants})
]);
