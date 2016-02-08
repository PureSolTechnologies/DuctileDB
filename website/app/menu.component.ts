import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

@Component({
    selector: 'menu',
    directives: [ROUTER_DIRECTIVES],
    template:
`<div class="navbar navbar-light bg-faded">
  <ul class="nav navbar-nav">
    <li class="nav-item">
      <a class="nav-link" [routerLink]="['/Features']">Features</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" [routerLink]="['/Documentation']">Documentation</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" [routerLink]="['/About']">About</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" [routerLink]="['/Contribute']">Contribute</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" [routerLink]="['/Imprint']">Imprint</a>
    </li>
  </ul>
</div>`
})
export class MenuComponent {
}
