import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

@Component({
    selector: 'menu',
    directives: [ROUTER_DIRECTIVES],
    template: `
<div class="navbar navbar-light bg-faded">
  <a class="navbar-brand" href="http://ductiledb.com">DuctileDB</a>
  <ul class="nav navbar-nav">
    <a class="item active" [routerLink]="['/Home']">Home</a>
    <a class="item" [routerLink]="['/Imprint']">Imprint</a>
    <a class="item">Upcoming Events</a>
  </ul>
</div>` 
})
export class MenuComponent {
}
