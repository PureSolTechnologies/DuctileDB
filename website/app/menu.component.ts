import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

@Component({
    selector: 'menu',
    directives: [ROUTER_DIRECTIVES],
    template:
`<div class="navbar navbar-light bg-faded">
  <a class="navbar-brand" href="http://ductiledb.com">DuctileDB</a>
  <ul class="nav navbar-nav">
    <li class="nav-item">
      <a class="nav-link" [routerLink]="['/Home']">Home</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" [routerLink]="['/Imprint']">Imprint</a>
    </li>
  </ul>
</div>`
})
export class MenuComponent {
}
