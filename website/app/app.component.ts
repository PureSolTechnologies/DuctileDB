import {Component} from 'angular2/core';
import {bootstrap} from 'angular2/platform/browser';
import {
  ROUTER_DIRECTIVES,
  RouteConfig
} from 'angular2/router';

import {HeaderComponent} from './header.component';
import {FooterComponent} from './footer.component';
import {MenuComponent} from './menu.component';
import {HomeComponent} from './home.component';
import {FeaturesComponent} from './features.component';
import {DocumentationComponent} from './documentation.component';
import {AboutComponent} from './about.component';
import {ContributeComponent} from './contribute.component';
import {ImprintComponent} from './imprint.component';

@Component({
    selector: 'app',
    directives: [
      MenuComponent,
      HeaderComponent,
      FooterComponent,
      ROUTER_DIRECTIVES
    ],
    template:
`<div class="container">
  <header></header>
  <menu></menu>
  <router-outlet></router-outlet>
  <footer></footer>
</div>`
})
@RouteConfig([
{ path: '/', name: 'root', redirectTo: ['/Home'] },
{ path: '/home', name: 'Home', component: HomeComponent },
{ path: '/documentation', name: 'Documentation', component: DocumentationComponent },
{ path: '/features', name: 'Features', component: FeaturesComponent },
{ path: '/about', name: 'About', component: AboutComponent },
{ path: '/contribute', name: 'Contribute', component: ContributeComponent },
{ path: '/imprint', name: 'Imprint', component: ImprintComponent },
])
export class AppComponent {
}
