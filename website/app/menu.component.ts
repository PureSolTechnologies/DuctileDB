import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

@Component({
    selector: 'menu',
    directives: [ROUTER_DIRECTIVES],
    template:
    `<div class="ui three item menu">
        <a class="item active" [routerLink]="['/Home']">Home</a>
     	<a class="item" [routerLink]="['/Imprint']">Imprint</a>
  	<a class="item">Upcoming Events</a>
</div>` 
})
export class MenuComponent {
}
